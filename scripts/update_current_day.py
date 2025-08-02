#!/usr/bin/env python3
"""
update_current_day.py
Update today's Walmart stock data in Snowflake with live data from Yahoo Finance
Now includes market cap data
"""

import yfinance as yf
import snowflake.connector
from datetime import datetime, timedelta
import logging
import os
import sys
import pytz

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_market_status():
    """Check if US market is open"""
    eastern = pytz.timezone('US/Eastern')
    now = datetime.now(eastern)
    
    # Market hours: 9:30 AM - 4:00 PM ET
    market_open = now.replace(hour=9, minute=30, second=0, microsecond=0)
    market_close = now.replace(hour=16, minute=0, second=0, microsecond=0)
    
    # Check if weekend
    if now.weekday() >= 5:  # Saturday = 5, Sunday = 6
        return False, "WEEKEND", now
    
    # Check market hours
    if now < market_open:
        return False, "PRE_MARKET", now
    elif now > market_close:
        return False, "AFTER_HOURS", now
    else:
        return True, "MARKET_OPEN", now

def get_yahoo_finance_data():
    """Fetch current Walmart data from Yahoo Finance"""
    try:
        logger.info("Fetching Walmart data from Yahoo Finance...")
        wmt = yf.Ticker("WMT")
        
        # Get today's data
        today = datetime.now().strftime('%Y-%m-%d')
        hist = wmt.history(period="5d")  # Get last 5 days to ensure we have data
        
        if hist.empty:
            logger.error("No data received from Yahoo Finance")
            return None
        
        # Get the most recent trading day's data
        latest_data = hist.iloc[-1]
        latest_date = hist.index[-1].strftime('%Y-%m-%d')
        
        # Get info including market cap
        info = wmt.info
        current_price = info.get('currentPrice', latest_data['Close'])
        market_cap = info.get('marketCap', 0)
        market_cap_billions = market_cap / 1_000_000_000
        
        # Calculate previous close (from the day before)
        if len(hist) >= 2:
            previous_close = hist.iloc[-2]['Close']
        else:
            previous_close = info.get('previousClose', latest_data['Open'])
        
        # Calculate price changes
        price_change = current_price - previous_close
        price_change_pct = (price_change / previous_close) * 100 if previous_close > 0 else 0
        
        # Prepare data
        data = {
            'date': latest_date,
            'open': round(latest_data['Open'], 2),
            'high': round(latest_data['High'], 2),
            'low': round(latest_data['Low'], 2),
            'close': round(latest_data['Close'], 2),
            'volume': int(latest_data['Volume']),
            'current_price': round(current_price, 2),
            'previous_close': round(previous_close, 2),
            'price_change': round(price_change, 2),
            'price_change_pct': round(price_change_pct, 4),
            'intraday_high': round(latest_data['High'], 2),
            'intraday_low': round(latest_data['Low'], 2),
            'market_cap_billions': round(market_cap_billions, 3)  # Store as 785.992
        }
        
        # Calculate moving averages
        if len(hist) >= 50:
            ma50 = hist['Close'].rolling(window=50).mean().iloc[-1]
            data['ma50'] = round(ma50, 2)
        else:
            data['ma50'] = None
            
        if len(hist) >= 200:
            ma200 = hist['Close'].rolling(window=200).mean().iloc[-1]
            data['ma200'] = round(ma200, 2)
        else:
            data['ma200'] = None
        
        logger.info(f"✅ Retrieved data for {latest_date}: ${current_price:.2f} ({price_change:+.2f}, {price_change_pct:+.2f}%)")
        logger.info(f"   Market Cap: ${market_cap_billions:.3f}B")
        return data
        
    except Exception as e:
        logger.error(f"Error fetching Yahoo Finance data: {e}")
        return None

def update_snowflake(data):
    """Update Snowflake with the latest data"""
    if not data:
        return False
    
    try:
        # Connect to Snowflake
        logger.info("Connecting to Snowflake...")
        conn = snowflake.connector.connect(
            user=os.environ['SNOWFLAKE_USER'],
            password=os.environ['SNOWFLAKE_PASSWORD'],
            account=os.environ['SNOWFLAKE_ACCOUNT'],
            warehouse=os.environ.get('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH'),
            database=os.environ.get('SNOWFLAKE_DATABASE', 'WALMART_STOCK_DB'),
            schema=os.environ.get('SNOWFLAKE_SCHEMA', 'PUBLIC'),
            login_timeout=60,
            network_timeout=60,
            socket_timeout=60
        )
        cursor = conn.cursor()
        logger.info("✅ Connected to Snowflake")
        
        # Add market cap column if it doesn't exist
        try:
            cursor.execute("ALTER TABLE WALMART_STOCK_DATA ADD COLUMN IF NOT EXISTS MARKET_CAP_BILLIONS FLOAT")
            conn.commit()
        except:
            pass  # Column already exists
        
        # Check if record exists for today
        cursor.execute(
            "SELECT UPDATE_COUNT FROM WALMART_STOCK_DATA WHERE DATE = %s",
            (data['date'],)
        )
        result = cursor.fetchone()
        
        if result:
            # Update existing record
            update_count = result[0] + 1
            cursor.execute("""
                UPDATE WALMART_STOCK_DATA SET
                    OPEN = %s,
                    HIGH = GREATEST(HIGH, %s),
                    LOW = LEAST(LOW, %s),
                    CLOSE = %s,
                    VOLUME = %s,
                    MA50 = COALESCE(%s, MA50),
                    MA200 = COALESCE(%s, MA200),
                    CURRENT_PRICE = %s,
                    UPDATE_COUNT = %s,
                    IS_LIVE_DATA = TRUE,
                    PREVIOUS_CLOSE = %s,
                    PRICE_CHANGE = %s,
                    PRICE_CHANGE_PCT = %s,
                    INTRADAY_HIGH = GREATEST(COALESCE(INTRADAY_HIGH, 0), %s),
                    INTRADAY_LOW = LEAST(COALESCE(INTRADAY_LOW, 999999), %s),
                    MARKET_CAP_BILLIONS = %s,
                    LAST_UPDATE_TIME = CURRENT_TIMESTAMP
                WHERE DATE = %s
            """, (
                data['open'], data['high'], data['low'], data['close'],
                data['volume'], data['ma50'], data['ma200'],
                data['current_price'], update_count,
                data['previous_close'], data['price_change'], data['price_change_pct'],
                data['intraday_high'], data['intraday_low'],
                data['market_cap_billions'],
                data['date']
            ))
            logger.info(f"✅ Updated record for {data['date']} (update #{update_count})")
        else:
            # Insert new record
            cursor.execute("""
                INSERT INTO WALMART_STOCK_DATA 
                (DATE, OPEN, HIGH, LOW, CLOSE, VOLUME, MA50, MA200,
                 CURRENT_PRICE, UPDATE_COUNT, IS_LIVE_DATA, PREVIOUS_CLOSE,
                 PRICE_CHANGE, PRICE_CHANGE_PCT, INTRADAY_HIGH, INTRADAY_LOW,
                 MARKET_CAP_BILLIONS, LAST_UPDATE_TIME)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 1, TRUE, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
            """, (
                data['date'], data['open'], data['high'], data['low'],
                data['close'], data['volume'], data['ma50'], data['ma200'],
                data['current_price'], data['previous_close'],
                data['price_change'], data['price_change_pct'],
                data['intraday_high'], data['intraday_low'],
                data['market_cap_billions']
            ))
            logger.info(f"✅ Inserted new record for {data['date']}")
        
        conn.commit()
        
        # Log summary for GitHub Actions
        print(f"\n{'='*60}")
        print(f"UPDATE SUMMARY - {data['date']}")
        print(f"{'='*60}")
        print(f"Current Price: ${data['current_price']:.2f}")
        print(f"Change: ${data['price_change']:+.2f} ({data['price_change_pct']:+.2f}%)")
        print(f"Day Range: ${data['low']:.2f} - ${data['high']:.2f}")
        print(f"Volume: {data['volume']:,}")
        print(f"Market Cap: ${data['market_cap_billions']:.3f}B")
        if data['ma50']:
            print(f"MA50: ${data['ma50']:.2f}")
        if data['ma200']:
            print(f"MA200: ${data['ma200']:.2f}")
        print(f"{'='*60}\n")
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"Snowflake error: {e}")
        if 'conn' in locals():
            conn.rollback()
        return False

def main():
    """Main execution function"""
    logger.info("="*60)
    logger.info("WALMART STOCK DATA UPDATE - YAHOO FINANCE")
    logger.info("="*60)
    
    # Check market status
    is_open, status, current_time = get_market_status()
    logger.info(f"Market Status: {status}")
    logger.info(f"Current Time (ET): {current_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Always try to update data (useful for after-hours or if running late)
    data = get_yahoo_finance_data()
    
    if data:
        success = update_snowflake(data)
        if success:
            logger.info("✅ Update completed successfully!")
            sys.exit(0)
        else:
            logger.error("❌ Update failed!")
            sys.exit(1)
    else:
        logger.error("❌ No data retrieved from Yahoo Finance")
        sys.exit(1)

if __name__ == "__main__":
    main()
