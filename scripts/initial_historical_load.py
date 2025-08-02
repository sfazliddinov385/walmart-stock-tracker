
"""
initial_historical_load.py
ONE-TIME script to load all historical Walmart stock data from Yahoo Finance to Snowflake
Run this ONCE to populate your database with historical data from 1972 to present
"""

import yfinance as yf
import snowflake.connector
import pandas as pd
from datetime import datetime
import logging
import os
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def extract_all_historical_data():
    """Extract ALL historical Walmart data from Yahoo Finance"""
    try:
        logger.info("="*60)
        logger.info("ONE-TIME HISTORICAL DATA EXTRACTION")
        logger.info("="*60)
        
        logger.info("Fetching ALL Walmart historical data from Yahoo Finance...")
        wmt = yf.Ticker("WMT")
        
        # Get maximum available history
        hist = wmt.history(period="max")
        
        if hist.empty:
            logger.error("No data received from Yahoo Finance")
            return None
            
        logger.info(f"✅ Downloaded {len(hist)} days of historical data")
        logger.info(f"Date range: {hist.index[0].date()} to {hist.index[-1].date()}")
        
        # Reset index to get Date as column
        hist.reset_index(inplace=True)
        
        # Create dataframe with all required columns
        df = pd.DataFrame()
        
        # Basic OHLCV data
        df['DATE'] = pd.to_datetime(hist['Date']).dt.strftime('%Y-%m-%d')
        df['OPEN'] = hist['Open'].round(2)
        df['HIGH'] = hist['High'].round(2)
        df['LOW'] = hist['Low'].round(2)
        df['CLOSE'] = hist['Close'].round(2)
        df['VOLUME'] = hist['Volume'].astype(int)
        
        # Calculate moving averages
        logger.info("Calculating moving averages...")
        df['MA50'] = hist['Close'].rolling(window=50).mean().round(2)
        df['MA200'] = hist['Close'].rolling(window=200).mean().round(2)
        
        # For historical data, set these to match daily values
        df['CURRENT_PRICE'] = df['CLOSE']
        df['UPDATE_COUNT'] = 0  # Historical data hasn't been updated
        df['IS_LIVE_DATA'] = False  # This is historical, not live
        
        # Calculate previous close and changes
        df['PREVIOUS_CLOSE'] = df['CLOSE'].shift(1)
        df['PRICE_CHANGE'] = (df['CLOSE'] - df['PREVIOUS_CLOSE']).round(2)
        df['PRICE_CHANGE_PCT'] = ((df['PRICE_CHANGE'] / df['PREVIOUS_CLOSE']) * 100).round(4)
        
        # Intraday values same as daily for historical
        df['INTRADAY_HIGH'] = df['HIGH']
        df['INTRADAY_LOW'] = df['LOW']
        
        # Timestamp
        df['LAST_UPDATE_TIME'] = datetime.now()
        
        # Handle NaN values in first row
        df['PREVIOUS_CLOSE'].fillna(0, inplace=True)
        df['PRICE_CHANGE'].fillna(0, inplace=True)
        df['PRICE_CHANGE_PCT'].fillna(0, inplace=True)
        
        return df
        
    except Exception as e:
        logger.error(f"Error extracting data: {e}")
        return None

def load_to_snowflake(df):
    """Load historical data to Snowflake"""
    if df is None or df.empty:
        logger.error("No data to load")
        return False
    
    try:
        # Connect to Snowflake
        logger.info("Connecting to Snowflake...")
        conn = snowflake.connector.connect(
            user=os.environ.get('SNOWFLAKE_USER'),
            password=os.environ.get('SNOWFLAKE_PASSWORD'),
            account=os.environ.get('SNOWFLAKE_ACCOUNT'),
            warehouse=os.environ.get('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH'),
            database=os.environ.get('SNOWFLAKE_DATABASE', 'WALMART_STOCK_DB'),
            schema=os.environ.get('SNOWFLAKE_SCHEMA', 'PUBLIC'),
            login_timeout=60,
            network_timeout=60,
            socket_timeout=60
        )
        cursor = conn.cursor()
        logger.info("✅ Connected to Snowflake")
        
        # Create table if it doesn't exist
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS WALMART_STOCK_DATA (
                DATE DATE NOT NULL PRIMARY KEY,
                OPEN FLOAT,
                HIGH FLOAT,
                LOW FLOAT,
                CLOSE FLOAT,
                VOLUME INTEGER,
                MA50 FLOAT,
                MA200 FLOAT,
                CURRENT_PRICE FLOAT,
                UPDATE_COUNT INTEGER DEFAULT 0,
                IS_LIVE_DATA BOOLEAN DEFAULT FALSE,
                PREVIOUS_CLOSE FLOAT,
                PRICE_CHANGE FLOAT,
                PRICE_CHANGE_PCT FLOAT,
                INTRADAY_HIGH FLOAT,
                INTRADAY_LOW FLOAT,
                LAST_UPDATE_TIME TIMESTAMP_NTZ
            )
        """)
        logger.info("✅ Table verified/created")
        
        # Clear existing data (optional - comment out if you want to preserve existing)
        logger.info("⚠️  Clearing existing data...")
        cursor.execute("TRUNCATE TABLE WALMART_STOCK_DATA")
        
        # Prepare data for bulk insert
        data_tuples = []
        for _, row in df.iterrows():
            data_tuples.append((
                row['DATE'],
                float(row['OPEN']),
                float(row['HIGH']),
                float(row['LOW']),
                float(row['CLOSE']),
                int(row['VOLUME']),
                float(row['MA50']) if pd.notna(row['MA50']) else None,
                float(row['MA200']) if pd.notna(row['MA200']) else None,
                float(row['CURRENT_PRICE']),
                int(row['UPDATE_COUNT']),
                bool(row['IS_LIVE_DATA']),
                float(row['PREVIOUS_CLOSE']),
                float(row['PRICE_CHANGE']),
                float(row['PRICE_CHANGE_PCT']),
                float(row['INTRADAY_HIGH']),
                float(row['INTRADAY_LOW']),
                row['LAST_UPDATE_TIME']
            ))
        
        # Insert in batches
        batch_size = 1000
        total_inserted = 0
        
        logger.info(f"Starting bulk insert of {len(data_tuples)} records...")
        
        insert_sql = """
            INSERT INTO WALMART_STOCK_DATA 
            (DATE, OPEN, HIGH, LOW, CLOSE, VOLUME, MA50, MA200,
             CURRENT_PRICE, UPDATE_COUNT, IS_LIVE_DATA, PREVIOUS_CLOSE,
             PRICE_CHANGE, PRICE_CHANGE_PCT, INTRADAY_HIGH, INTRADAY_LOW,
             LAST_UPDATE_TIME)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        for i in range(0, len(data_tuples), batch_size):
            batch = data_tuples[i:i + batch_size]
            cursor.executemany(insert_sql, batch)
            conn.commit()
            
            total_inserted += len(batch)
            progress = (total_inserted / len(data_tuples)) * 100
            logger.info(f"Progress: {total_inserted}/{len(data_tuples)} records ({progress:.1f}%)")
        
        logger.info("✅ All historical data loaded successfully!")
        
        # Verify the load
        cursor.execute("SELECT COUNT(*) FROM WALMART_STOCK_DATA")
        count = cursor.fetchone()[0]
        logger.info(f"Total records in database: {count}")
        
        # Show summary
        cursor.execute("""
            SELECT 
                MIN(DATE) as earliest_date,
                MAX(DATE) as latest_date,
                COUNT(*) as total_records,
                COUNT(MA50) as records_with_ma50,
                COUNT(MA200) as records_with_ma200
            FROM WALMART_STOCK_DATA
        """)
        
        result = cursor.fetchone()
        print("\n" + "="*60)
        print("LOAD SUMMARY")
        print("="*60)
        print(f"Earliest Date: {result[0]}")
        print(f"Latest Date: {result[1]}")
        print(f"Total Records: {result[2]:,}")
        print(f"Records with MA50: {result[3]:,}")
        print(f"Records with MA200: {result[4]:,}")
        print("="*60)
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"Database error: {e}")
        if 'conn' in locals():
            conn.rollback()
        return False

def main():
    """Main execution"""
    print("""
    ╔══════════════════════════════════════════════════════════╗
    ║         ONE-TIME HISTORICAL DATA LOAD                    ║
    ║                                                          ║
    ║  This script will:                                       ║
    ║  1. Download ALL Walmart stock data (1972-present)      ║
    ║  2. Calculate moving averages                           ║
    ║  3. CLEAR existing data in Snowflake                    ║
    ║  4. Load all historical data                            ║
    ║                                                          ║
    ║  ⚠️  WARNING: This will REPLACE all existing data!      ║
    ╚══════════════════════════════════════════════════════════╝
    """)
    
    response = input("\nDo you want to continue? (yes/no): ")
    if response.lower() != 'yes':
        logger.info("Operation cancelled")
        sys.exit(0)
    
    # Check for required environment variables
    required_vars = ['SNOWFLAKE_USER', 'SNOWFLAKE_PASSWORD', 'SNOWFLAKE_ACCOUNT']
    missing_vars = [var for var in required_vars if not os.environ.get(var)]
    
    if missing_vars:
        logger.error(f"Missing required environment variables: {', '.join(missing_vars)}")
        logger.info("Please set these variables or create a .env file")
        sys.exit(1)
    
    # Extract data
    logger.info("\nStarting historical data extraction...")
    df = extract_all_historical_data()
    
    if df is not None:
        # Save backup locally
        backup_file = f"walmart_historical_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        df.to_csv(backup_file, index=False)
        logger.info(f"✅ Backup saved to: {backup_file}")
        
        # Load to Snowflake
        logger.info("\nLoading data to Snowflake...")
        if load_to_snowflake(df):
            logger.info("\n✅ ONE-TIME HISTORICAL LOAD COMPLETED SUCCESSFULLY!")
            logger.info("You can now use the GitHub Actions workflow to keep data updated daily.")
        else:
            logger.error("\n❌ Failed to load data to Snowflake")
            sys.exit(1)
    else:
        logger.error("\n❌ Failed to extract historical data")
        sys.exit(1)

if __name__ == "__main__":
    # Optional: Load from .env file if it exists
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass
    
    main()