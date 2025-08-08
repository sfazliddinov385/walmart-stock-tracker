#!/usr/bin/env python3
"""
alert_system.py - Email Alert System for Walmart Stock Monitoring
Sends alerts for price movements, technical indicators, and trading signals
"""

import smtplib
import snowflake.connector
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta
import os
import logging
import json
import traceback
from typing import Dict, List, Optional, Tuple

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class StockAlertSystem:
    """Alert system for monitoring Walmart stock and sending email notifications"""
    
    def __init__(self):
        """Initialize alert system with configuration"""
        # Email configuration - Fixed to handle empty SMTP_SERVER
        smtp_server = os.environ.get('SMTP_SERVER', 'smtp.gmail.com')
        self.smtp_server = smtp_server if smtp_server and smtp_server.strip() else 'smtp.gmail.com'
        # Fixed: Handle empty string or missing SMTP_PORT
        smtp_port_str = os.environ.get('SMTP_PORT', '587')
        self.smtp_port = int(smtp_port_str) if smtp_port_str and smtp_port_str.strip() else 587
        
        self.sender_email = os.environ.get('SENDER_EMAIL')
        self.sender_password = os.environ.get('SENDER_PASSWORD')  # App-specific password for Gmail
        recipient_str = os.environ.get('RECIPIENT_EMAILS', '')
        self.recipient_emails = [email.strip() for email in recipient_str.split(',') if email.strip()]
        
        # Alert thresholds (can be customized via environment variables)
        # Fixed: Handle empty strings for all threshold values
        price_threshold = os.environ.get('PRICE_CHANGE_THRESHOLD', '2.0')
        self.price_change_threshold = float(price_threshold) if price_threshold and price_threshold.strip() else 2.0
        
        volume_threshold = os.environ.get('VOLUME_SPIKE_THRESHOLD', '1.5')
        self.volume_spike_threshold = float(volume_threshold) if volume_threshold and volume_threshold.strip() else 1.5
        
        rsi_oversold_str = os.environ.get('RSI_OVERSOLD', '30')
        self.rsi_oversold = float(rsi_oversold_str) if rsi_oversold_str and rsi_oversold_str.strip() else 30.0
        
        rsi_overbought_str = os.environ.get('RSI_OVERBOUGHT', '70')
        self.rsi_overbought = float(rsi_overbought_str) if rsi_overbought_str and rsi_overbought_str.strip() else 70.0
        
        # Track sent alerts to avoid duplicates
        self.alert_history_file = 'alert_history.json'
        self.alert_history = self.load_alert_history()
        
        # Log configuration
        logger.info(f"SMTP Server: {self.smtp_server}:{self.smtp_port}")
        logger.info(f"Sender Email: {self.sender_email}")
        logger.info(f"Recipients: {self.recipient_emails}")
        
    def load_alert_history(self) -> Dict:
        """Load alert history from file"""
        if os.path.exists(self.alert_history_file):
            try:
                with open(self.alert_history_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"Could not load alert history: {e}")
        return {}
    
    def save_alert_history(self):
        """Save alert history to file"""
        try:
            with open(self.alert_history_file, 'w') as f:
                json.dump(self.alert_history, f, indent=2)
        except Exception as e:
            logger.error(f"Could not save alert history: {e}")
    
    def get_latest_data(self) -> Optional[Dict]:
        """Fetch latest stock data from Snowflake"""
        try:
            conn = snowflake.connector.connect(
                user=os.environ['SNOWFLAKE_USER'],
                password=os.environ['SNOWFLAKE_PASSWORD'],
                account=os.environ['SNOWFLAKE_ACCOUNT'],
                warehouse=os.environ.get('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH'),
                database=os.environ.get('SNOWFLAKE_DATABASE', 'WALMART_STOCK_DB'),
                schema=os.environ.get('SNOWFLAKE_SCHEMA', 'PUBLIC')
            )
            cursor = conn.cursor()
            
            # Get latest data with comparison to previous day
            query = """
                WITH latest_data AS (
                    SELECT 
                        DATE,
                        OPEN,
                        HIGH,
                        LOW,
                        CLOSE,
                        VOLUME,
                        CURRENT_PRICE,
                        PRICE_CHANGE,
                        PRICE_CHANGE_PCT,
                        MA50,
                        MA200,
                        RSI_14,
                        FIFTY_TWO_WEEK_HIGH,
                        FIFTY_TWO_WEEK_LOW,
                        VOLUME_MA_20,
                        VOLUME_RATIO,
                        PCT_FROM_52W_HIGH,
                        PCT_FROM_52W_LOW,
                        MARKET_CAP_BILLIONS,
                        MARKET_STATUS,
                        LAST_UPDATE_TIME
                    FROM WALMART_STOCK_DATA
                    ORDER BY DATE DESC
                    LIMIT 2
                )
                SELECT * FROM latest_data ORDER BY DATE DESC
            """
            
            cursor.execute(query)
            results = cursor.fetchall()
            columns = [desc[0].lower() for desc in cursor.description]
            
            cursor.close()
            conn.close()
            
            if results:
                # Convert to dictionary
                latest = dict(zip(columns, results[0]))
                previous = dict(zip(columns, results[1])) if len(results) > 1 else None
                
                return {
                    'current': latest,
                    'previous': previous
                }
            
            return None
            
        except Exception as e:
            logger.error(f"Error fetching data from Snowflake: {e}")
            return None
    
    def check_alerts(self, data: Dict) -> List[Dict]:
        """Check for various alert conditions"""
        alerts = []
        current = data['current']
        previous = data['previous'] if data.get('previous') else None
        
        # 1. Large Price Movement Alert
        if current.get('price_change_pct') and abs(current['price_change_pct']) >= self.price_change_threshold:
            direction = 'up' if current['price_change_pct'] > 0 else 'down'
            alerts.append({
                'type': 'PRICE_MOVEMENT',
                'severity': 'HIGH',
                'title': f'🚨 Large Price Movement: {direction.upper()} {abs(current["price_change_pct"]):.2f}%',
                'message': f'WMT is {direction} ${abs(current["price_change"]):.2f} ({current["price_change_pct"]:+.2f}%) to ${current["current_price"]:.2f}'
            })
        
        # 2. Volume Spike Alert
        if current.get('volume_ratio') and current['volume_ratio'] >= self.volume_spike_threshold:
            alerts.append({
                'type': 'VOLUME_SPIKE',
                'severity': 'MEDIUM',
                'title': f'📊 Unusual Volume: {current["volume_ratio"]:.2f}x Average',
                'message': f'Trading volume is {current["volume_ratio"]:.2f}x the 20-day average ({current["volume"]:,} shares)'
            })
        
        # 3. RSI Alerts
        if current.get('rsi_14'):
            if current['rsi_14'] <= self.rsi_oversold:
                alerts.append({
                    'type': 'RSI_OVERSOLD',
                    'severity': 'HIGH',
                    'title': f'🟢 RSI Oversold Signal: {current["rsi_14"]:.2f}',
                    'message': f'RSI(14) is {current["rsi_14"]:.2f}, indicating potential buying opportunity'
                })
            elif current['rsi_14'] >= self.rsi_overbought:
                alerts.append({
                    'type': 'RSI_OVERBOUGHT',
                    'severity': 'MEDIUM',
                    'title': f'🔴 RSI Overbought Signal: {current["rsi_14"]:.2f}',
                    'message': f'RSI(14) is {current["rsi_14"]:.2f}, indicating potential selling pressure'
                })
        
        # 4. Moving Average Crossover Alerts
        if current.get('ma50') and current.get('ma200'):
            current_price = current['current_price']
            
            # Golden Cross (50-day crosses above 200-day)
            if previous and previous.get('ma50') and previous.get('ma200'):
                if (previous['ma50'] <= previous['ma200'] and 
                    current['ma50'] > current['ma200']):
                    alerts.append({
                        'type': 'GOLDEN_CROSS',
                        'severity': 'HIGH',
                        'title': '🌟 Golden Cross Signal',
                        'message': 'MA50 crossed above MA200 - Bullish signal'
                    })
                # Death Cross (50-day crosses below 200-day)
                elif (previous['ma50'] >= previous['ma200'] and 
                      current['ma50'] < current['ma200']):
                    alerts.append({
                        'type': 'DEATH_CROSS',
                        'severity': 'HIGH',
                        'title': '💀 Death Cross Signal',
                        'message': 'MA50 crossed below MA200 - Bearish signal'
                    })
            
            # Price vs Moving Averages
            if current_price > current['ma50'] and current_price > current['ma200']:
                if previous and previous.get('current_price') and previous.get('ma200'):
                    if previous['current_price'] <= previous['ma200']:
                        alerts.append({
                            'type': 'BREAKOUT',
                            'severity': 'MEDIUM',
                            'title': '📈 Breakout Above MA200',
                            'message': f'Price (${current_price:.2f}) broke above MA200 (${current["ma200"]:.2f})'
                        })
        
        # 5. 52-Week High/Low Alerts
        if current.get('pct_from_52w_high') and current['pct_from_52w_high'] >= -1:
            alerts.append({
                'type': '52W_HIGH',
                'severity': 'HIGH',
                'title': '🎯 Near 52-Week High',
                'message': f'Price is within 1% of 52-week high (${current.get("fifty_two_week_high", 0):.2f})'
            })
        elif current.get('pct_from_52w_low') and current['pct_from_52w_low'] <= 5:
            alerts.append({
                'type': '52W_LOW',
                'severity': 'HIGH',
                'title': '⚠️ Near 52-Week Low',
                'message': f'Price is within 5% of 52-week low (${current.get("fifty_two_week_low", 0):.2f})'
            })
        
        # 6. Gap Up/Down Alert
        if previous and current.get('open') and previous.get('close'):
            gap_pct = ((current['open'] - previous['close']) / previous['close']) * 100
            if abs(gap_pct) >= 1:  # 1% gap threshold
                gap_type = 'up' if gap_pct > 0 else 'down'
                alerts.append({
                    'type': f'GAP_{gap_type.upper()}',
                    'severity': 'MEDIUM',
                    'title': f'📍 Gap {gap_type.capitalize()}: {abs(gap_pct):.2f}%',
                    'message': f'Stock opened with a {abs(gap_pct):.2f}% gap {gap_type} from previous close'
                })
        
        return alerts
    
    def should_send_alert(self, alert: Dict) -> bool:
        """Check if alert should be sent (avoid duplicates)"""
        today = datetime.now().strftime('%Y-%m-%d')
        alert_key = f"{today}_{alert['type']}"
        
        # Check if this alert was already sent today
        if alert_key in self.alert_history:
            last_sent = datetime.fromisoformat(self.alert_history[alert_key])
            # Don't send same alert type more than once per day
            if (datetime.now() - last_sent).total_seconds() < 86400:
                return False
        
        return True
    
    def format_email_html(self, alerts: List[Dict], data: Dict) -> str:
        """Format alerts as HTML email"""
        current = data['current']
        
        # Group alerts by severity
        high_alerts = [a for a in alerts if a['severity'] == 'HIGH']
        medium_alerts = [a for a in alerts if a['severity'] == 'MEDIUM']
        
        # Handle None values with defaults
        current_price = current.get('current_price', 0)
        price_change = current.get('price_change', 0)
        price_change_pct = current.get('price_change_pct', 0)
        volume = current.get('volume', 0)
        
        html = f"""
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; background-color: #f4f4f4; padding: 20px; }}
                .container {{ background-color: white; border-radius: 10px; padding: 20px; max-width: 600px; margin: 0 auto; }}
                .header {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 20px; border-radius: 10px 10px 0 0; margin: -20px -20px 20px -20px; }}
                .walmart-logo {{ font-size: 24px; font-weight: bold; }}
                .price-info {{ background-color: #f8f9fa; padding: 15px; border-radius: 8px; margin: 20px 0; }}
                .alert-high {{ border-left: 4px solid #dc3545; padding: 10px; margin: 10px 0; background-color: #fff5f5; }}
                .alert-medium {{ border-left: 4px solid #ffc107; padding: 10px; margin: 10px 0; background-color: #fffdf5; }}
                .metric {{ display: inline-block; margin: 10px 20px 10px 0; }}
                .metric-label {{ color: #666; font-size: 12px; }}
                .metric-value {{ font-size: 18px; font-weight: bold; color: #333; }}
                .footer {{ margin-top: 30px; padding-top: 20px; border-top: 1px solid #ddd; font-size: 12px; color: #666; }}
                .button {{ background-color: #007bff; color: white; padding: 10px 20px; text-decoration: none; border-radius: 5px; display: inline-block; margin: 10px 0; }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <div class="walmart-logo">🛒 Walmart Stock Alerts</div>
                    <div style="font-size: 14px; margin-top: 10px;">
                        {datetime.now().strftime('%B %d, %Y at %I:%M %p')}
                    </div>
                </div>
                
                <div class="price-info">
                    <div class="metric">
                        <div class="metric-label">Current Price</div>
                        <div class="metric-value">${current_price:.2f}</div>
                    </div>
                    <div class="metric">
                        <div class="metric-label">Change</div>
                        <div class="metric-value" style="color: {'green' if price_change >= 0 else 'red'};">
                            {price_change:+.2f} ({price_change_pct:+.2f}%)
                        </div>
                    </div>
                    <div class="metric">
                        <div class="metric-label">Volume</div>
                        <div class="metric-value">{volume:,.0f}</div>
                    </div>
                </div>
        """
        
        if high_alerts:
            html += "<h3>🚨 High Priority Alerts</h3>"
            for alert in high_alerts:
                html += f"""
                <div class="alert-high">
                    <strong>{alert['title']}</strong><br>
                    {alert['message']}
                </div>
                """
        
        if medium_alerts:
            html += "<h3>⚠️ Medium Priority Alerts</h3>"
            for alert in medium_alerts:
                html += f"""
                <div class="alert-medium">
                    <strong>{alert['title']}</strong><br>
                    {alert['message']}
                </div>
                """
        
        # Add key metrics with None handling
        ma50_display = f"${current.get('ma50', 0):.2f}" if current.get('ma50') else 'N/A'
        ma200_display = f"${current.get('ma200', 0):.2f}" if current.get('ma200') else 'N/A'
        
        html += f"""
                <h3>📊 Key Metrics</h3>
                <table style="width: 100%; border-collapse: collapse;">
                    <tr>
                        <td style="padding: 8px; border-bottom: 1px solid #ddd;">
                            <strong>RSI (14)</strong>
                        </td>
                        <td style="padding: 8px; border-bottom: 1px solid #ddd; text-align: right;">
                            {current.get('rsi_14', 'N/A')}
                        </td>
                    </tr>
                    <tr>
                        <td style="padding: 8px; border-bottom: 1px solid #ddd;">
                            <strong>MA50</strong>
                        </td>
                        <td style="padding: 8px; border-bottom: 1px solid #ddd; text-align: right;">
                            {ma50_display}
                        </td>
                    </tr>
                    <tr>
                        <td style="padding: 8px; border-bottom: 1px solid #ddd;">
                            <strong>MA200</strong>
                        </td>
                        <td style="padding: 8px; border-bottom: 1px solid #ddd; text-align: right;">
                            {ma200_display}
                        </td>
                    </tr>
                    <tr>
                        <td style="padding: 8px; border-bottom: 1px solid #ddd;">
                            <strong>52-Week Range</strong>
                        </td>
                        <td style="padding: 8px; border-bottom: 1px solid #ddd; text-align: right;">
                            ${current.get('fifty_two_week_low', 0):.2f} - ${current.get('fifty_two_week_high', 0):.2f}
                        </td>
                    </tr>
                    <tr>
                        <td style="padding: 8px;">
                            <strong>Volume Ratio</strong>
                        </td>
                        <td style="padding: 8px; text-align: right;">
                            {current.get('volume_ratio', 0):.2f}x
                        </td>
                    </tr>
                </table>
                
                <div style="text-align: center; margin-top: 30px;">
                    <a href="https://finance.yahoo.com/quote/WMT" class="button">View on Yahoo Finance</a>
                </div>
                
                <div class="footer">
                    <p>This is an automated alert from your Walmart Stock Monitoring System.</p>
                    <p>Alert thresholds: Price change ≥{self.price_change_threshold}% | Volume ≥{self.volume_spike_threshold}x | RSI ≤{self.rsi_oversold} or ≥{self.rsi_overbought}</p>
                    <p>To modify alert settings, update the environment variables in your configuration.</p>
                </div>
            </div>
        </body>
        </html>
        """
        
        return html
    
    def send_email(self, alerts: List[Dict], data: Dict):
        """Send email with alerts"""
        if not self.sender_email:
            logger.error(f"Sender email is missing!")
            return False
            
        if not self.recipient_emails:
            logger.error(f"Recipient emails are missing!")
            return False
            
        if not self.sender_password:
            logger.error(f"Sender password is missing!")
            return False
        
        try:
            # Create message
            msg = MIMEMultipart('alternative')
            msg['Subject'] = f"WMT Stock Alert: {alerts[0]['title']}"
            msg['From'] = self.sender_email
            msg['To'] = ', '.join(self.recipient_emails)
            
            # Create HTML content
            html_content = self.format_email_html(alerts, data)
            
            # Also create plain text version
            text_content = f"""
            WALMART STOCK ALERTS
            {datetime.now().strftime('%B %d, %Y at %I:%M %p')}
            
            Current Price: ${data['current'].get('current_price', 0):.2f}
            Change: {data['current'].get('price_change', 0):+.2f} ({data['current'].get('price_change_pct', 0):+.2f}%)
            
            ALERTS:
            """
            for alert in alerts:
                text_content += f"\n{alert['title']}\n{alert['message']}\n"
            
            # Attach parts
            part1 = MIMEText(text_content, 'plain')
            part2 = MIMEText(html_content, 'html')
            msg.attach(part1)
            msg.attach(part2)
            
            # Send email with detailed logging and error handling
            logger.info(f"Attempting to connect to {self.smtp_server}:{self.smtp_port}")
            
            # Try different connection methods
            try:
                # Method 1: Standard connection
                server = smtplib.SMTP(self.smtp_server, self.smtp_port)
                server.set_debuglevel(0)  # Set to 1 for debug output
                server.ehlo()
                server.starttls()
                server.ehlo()
            except Exception as e:
                logger.warning(f"Standard connection failed: {e}, trying alternative...")
                # Method 2: Direct SSL connection
                try:
                    import ssl
                    context = ssl.create_default_context()
                    server = smtplib.SMTP(self.smtp_server, self.smtp_port)
                    server.starttls(context=context)
                except Exception as e2:
                    logger.error(f"Alternative connection also failed: {e2}")
                    raise
            
            logger.info(f"Connected to SMTP server, attempting login...")
            server.login(self.sender_email, self.sender_password)
            logger.info(f"Login successful, sending email...")
            
            server.send_message(msg)
            server.quit()
            
            logger.info(f"✅ Email sent successfully to {', '.join(self.recipient_emails)}")
            
            # Update alert history
            for alert in alerts:
                alert_key = f"{datetime.now().strftime('%Y-%m-%d')}_{alert['type']}"
                self.alert_history[alert_key] = datetime.now().isoformat()
            self.save_alert_history()
            
            return True
            
        except smtplib.SMTPAuthenticationError as e:
            logger.error(f"❌ Authentication failed: {e}")
            logger.error("Please check:")
            logger.error("1. Your email in SENDER_EMAIL secret")
            logger.error("2. Your app password in SENDER_PASSWORD secret")
            logger.error("3. That 2-factor auth is enabled on your Gmail")
            return False
        except smtplib.SMTPException as e:
            logger.error(f"❌ SMTP error: {e}")
            return False
        except Exception as e:
            logger.error(f"❌ Failed to send email: {e}")
            logger.error(f"Error type: {type(e).__name__}")
            logger.error(traceback.format_exc())
            return False
    
    def run(self):
        """Main execution function"""
        logger.info("="*60)
        logger.info("WALMART STOCK ALERT SYSTEM")
        logger.info("="*60)
        
        # Fetch latest data
        data = self.get_latest_data()
        if not data:
            logger.error("Could not fetch stock data")
            return False
        
        current = data['current']
        logger.info(f"Current Price: ${current.get('current_price', 0):.2f}")
        logger.info(f"Change: {current.get('price_change_pct', 0):+.2f}%")
        
        # Check for alerts
        alerts = self.check_alerts(data)
        
        if not alerts:
            logger.info("No alert conditions met")
            return True
        
        # Filter alerts that should be sent
        alerts_to_send = [a for a in alerts if self.should_send_alert(a)]
        
        if not alerts_to_send:
            logger.info(f"Found {len(alerts)} alerts but all were recently sent")
            return True
        
        logger.info(f"Found {len(alerts_to_send)} new alerts to send:")
        for alert in alerts_to_send:
            logger.info(f"  - {alert['title']}")
        
        # Send email
        if self.send_email(alerts_to_send, data):
            logger.info("✅ Alert system completed successfully")
            return True
        else:
            logger.error("❌ Failed to send alerts")
            return False

def main():
    """Main entry point"""
    # Check required environment variables
    required_vars = [
        'SNOWFLAKE_USER', 'SNOWFLAKE_PASSWORD', 'SNOWFLAKE_ACCOUNT',
        'SENDER_EMAIL', 'SENDER_PASSWORD', 'RECIPIENT_EMAILS'
    ]
    
    missing_vars = [var for var in required_vars if not os.environ.get(var)]
    if missing_vars:
        logger.error(f"Missing required environment variables: {', '.join(missing_vars)}")
        logger.info("Please set these in your .env file or GitHub Secrets")
        return False
    
    # Run alert system
    alert_system = StockAlertSystem()
    return alert_system.run()

if __name__ == "__main__":
    import sys
    success = main()
    sys.exit(0 if success else 1)
