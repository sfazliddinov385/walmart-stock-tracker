#!/usr/bin/env python3
"""
update_current_day.py - Enhanced with Technical Indicators
Includes: RSI, 52-Week High/Low, Volume Ratio, Market Cap, and Market Status
"""

import yfinance as yf
import snowflake.connector
import pandas as pd
import numpy as np
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

def calculate_rsi(prices, period=14):
    """
    Calculate Relative Strength Index
    RSI = 100 - (100 / (1 + RS))
    RS = Average Gain / Average Loss
    """
    if len(prices) < period + 1:
        return None
        
    delta = prices.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    
    # Avoid division by zero
    rs = gain / loss.replace(0, 0.0001)
    rsi = 100 - (100 / (1 + rs))
    
    return rsi.iloc[-1] if not pd.isna(rsi.iloc[-1]) else None

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
    """Fetch current Walmart data from Yahoo Finance with technical indicators"""
    try:
        logger.info("Fetching Walmart data from Yahoo Finance...")
        wmt = yf.Ticker("WMT")
        
        # Get market status
        is_open, market_status, current_time = get_market_status()
        
        # Get 1 year of data for 52-week calculations
        hist_1y = wmt.history(period="1y")
        
        if hist_1y.empty:
            logger.error("No data received from Yahoo Finance")
            return None
        
        # Get the most recent trading day's data
        latest_data = hist_1y.iloc[-1]
        latest_date = hist_1y.index[-1].strftime('%Y-%m-%d')
        
        # Get info including market cap
        info = wmt.info
        current_price = info.get('currentPrice', latest_data['Close'])
        market_cap = info.get('marketCap', 0)
        market_cap_billions = market_cap / 1_000_000_000
        
        # Calculate previous close
        if len(hist_1y) >= 2:
            previous_close = hist_1y.iloc[-2]['Close']
        else:
            previous_close = info.get('previousClose', latest_data['Open'])
        
        # Calculate price changes
        price_change = current_price - previous_close
        price_change_pct = (price_change / previous_close) * 100 if previous_close > 0 else 0
        
        # Calculate RSI
        rsi_14 = calculate_rsi(hist_1y['Close'])
        
        # Calculate 52-week high and low
        fifty_two_week_high = hist_1y['High'].max()
        fifty_two_week_low = hist_1y['Low'].min()
        
        # Calculate percentage from 52-week high/low
        pct_from_52w_high = ((current_price - fifty_two_week_high) / fifty_two_week_high) * 100
        pct_from_52w_low = ((current_price - fifty_two_week_low) / fifty_two_week_low) * 100
        
        # Calculate volume metrics
        volume_ma_20 = hist_1y['Volume'].rolling(window=20).mean().iloc[-1]
        volume_ratio = latest_data['Volume'] / volume_ma_20 if volume_ma_20 > 0 else 1
        
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
            'market_cap_billions': round(market_cap_billions, 3),
            'market_status': market_status,
            # New technical indicators
            'rsi_14': round(rsi_14, 2) if rsi_14 else None,
            'fifty_two_week_high': round(fifty_two_week_high, 2),
            'fifty_two_week_low': round(fifty_two_week_low, 2),
            'volume_ma_20': int(volume_ma_20),
            'volume_ratio': round(volume_ratio, 2),
            'pct_from_52w_high': round(pct_from_52w_high, 2),
            'pct_from_52w_low': round(pct_from_52w_low, 2)
        }
        
        # Calculate moving averages
        if len(hist_1y) >= 50:
            ma50 = hist_1y['Close'].rolling(window=50).mean().iloc[-1]
            data['ma50'] = round(ma50, 2)
        else:
            data['ma50'] = None
            
        if len(hist_1y) >= 200:
            ma200 = hist_1y['Close'].rolling(window=200).mean().iloc[-1]
            data['ma200'] = round(ma200, 2)
        else:
            data['ma200'] = None
        
        logger.info(f"✅ Retrieved data for {latest_date}: ${current_price:.2f} ({price_change:+.2f}, {price_change_pct:+.2f}%)")
        logger.info(f"   Market Cap: ${market_cap_billions:.3f}B | Status: {market_status}")
        logger.info(f"   RSI: {rsi_14:.2f} | 52W Range: ${fifty_two_week_low:.2f}-${fifty_two_week_high:.2f}")
        logger.info(f"   Volume Ratio: {volume_ratio:.2f}x average")
        
        return data
        
    except Exception as e:
        logger.error(f"Error fetching Yahoo Finance data: {e}")
        return None

def update_snowflake(data):
    """Update Snowflake with the latest data including technical indicators"""
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
                    MARKET_STATUS = %s,
                    RSI_14 = %s,
                    FIFTY_TWO_WEEK_HIGH = %s,
                    FIFTY_TWO_WEEK_LOW = %s,
                    VOLUME_MA_20 = %s,
                    VOLUME_RATIO = %s,
                    PCT_FROM_52W_HIGH = %s,
                    PCT_FROM_52W_LOW = %s,
                    LAST_UPDATE_TIME = CURRENT_TIMESTAMP
                WHERE DATE = %s
            """, (
                data['open'], data['high'], data['low'], data['close'],
                data['volume'], data['ma50'], data['ma200'],
                data['current_price'], update_count,
                data['previous_close'], data['price_change'], data['price_change_pct'],
                data['intraday_high'], data['intraday_low'],
                data['market_cap_billions'], data['market_status'],
                data['rsi_14'], data['fifty_two_week_high'], data['fifty_two_week_low'],
                data['volume_ma_20'], data['volume_ratio'],
                data['pct_from_52w_high'], data['pct_from_52w_low'],
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
                 MARKET_CAP_BILLIONS, MARKET_STATUS, RSI_14, FIFTY_TWO_WEEK_HIGH,
                 FIFTY_TWO_WEEK_LOW, VOLUME_MA_20, VOLUME_RATIO, PCT_FROM_52W_HIGH,
                 PCT_FROM_52W_LOW, LAST_UPDATE_TIME)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 1, TRUE, %s, %s, %s, %s, %s, 
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
            """, (
                data['date'], data['open'], data['high'], data['low'],
                data['close'], data['volume'], data['ma50'], data['ma200'],
                data['current_price'], data['previous_close'],
                data['price_change'], data['price_change_pct'],
                data['intraday_high'], data['intraday_low'],
                data['market_cap_billions'], data['market_status'],
                data['rsi_14'], data['fifty_two_week_high'], data['fifty_two_week_low'],
                data['volume_ma_20'], data['volume_ratio'],
                data['pct_from_52w_high'], data['pct_from_52w_low']
            ))
            logger.info(f"✅ Inserted new record for {data['date']}")
        
        conn.commit()
        
        # Log summary for GitHub Actions
        print(f"\n{'='*70}")
        print(f"UPDATE SUMMARY - {data['date']}")
        print(f"{'='*70}")
        print(f"Current Price: ${data['current_price']:.2f}")
        print(f"Change: ${data['price_change']:+.2f} ({data['price_change_pct']:+.2f}%)")
        print(f"Day Range: ${data['low']:.2f} - ${data['high']:.2f}")
        print(f"Volume: {data['volume']:,} (Ratio: {data['volume_ratio']:.2f}x)")
        print(f"Market Cap: ${data['market_cap_billions']:.3f}B")
        print(f"Market Status: {data['market_status']}")
        print(f"\nTechnical Indicators:")
        print(f"RSI(14): {data['rsi_14'] or 'N/A'}")
        print(f"52-Week Range: ${data['fifty_two_week_low']:.2f} - ${data['fifty_two_week_high']:.2f}")
        print(f"% from 52W High: {data['pct_from_52w_high']:.2f}%")
        print(f"% from 52W Low: {data['pct_from_52w_low']:.2f}%")
        if data['ma50']:
            print(f"MA50: ${data['ma50']:.2f}")
        if data['ma200']:
            print(f"MA200: ${data['ma200']:.2f}")
        print(f"{'='*70}\n")
        
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