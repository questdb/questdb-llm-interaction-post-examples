#!/usr/bin/env python3

import requests
from datetime import datetime
from typing import Dict, List
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class QuestDBCryptoIngestion:
   """
   Handles crypto data ingestion into QuestDB via REST API
   """

   def __init__(self, questdb_host: str = "localhost", questdb_port: int = 9000):
       self.questdb_url = f"http://{questdb_host}:{questdb_port}"
       self.session = requests.Session()

   def create_crypto_table(self) -> bool:
       """
       Create the crypto_prices table with optimized schema for time-series data
       """
       create_table_sql = """
       CREATE TABLE IF NOT EXISTS crypto_prices (
           timestamp TIMESTAMP,
           symbol SYMBOL CAPACITY 256 CACHE,
           exchange SYMBOL CAPACITY 64 CACHE,
           price DOUBLE,
           volume DOUBLE,
           bid DOUBLE,
           ask DOUBLE,
           spread DOUBLE,
           market_cap DOUBLE
       ) TIMESTAMP(timestamp) PARTITION BY DAY WAL
       """

       try:
           response = self.session.get(
               f"{self.questdb_url}/exec",
               params={"query": create_table_sql}
           )
           response.raise_for_status()
           logger.info("✅ Crypto prices table created successfully")
           return True
       except Exception as e:
           logger.error(f"❌ Failed to create table: {e}")
           return False

   def fetch_binance_data(self, symbols: List[str]) -> List[Dict]:
       """
       Fetch real-time crypto data from Binance API with error handling
       """
       binance_data = []

       # Add headers to avoid blocking
       headers = {
           'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
       }

       for symbol in symbols:
           try:
               # Try simple price endpoint first (less likely to be blocked)
               simple_url = "https://api.binance.com/api/v3/ticker/price"
               response = requests.get(
                   simple_url,
                   params={"symbol": f"{symbol}USDT"},
                   headers=headers,
                   timeout=10
               )

               if response.status_code == 451:
                   logger.warning(f"⚠️  Binance API blocked (451 error) for {symbol}. Skipping Binance.")
                   break  # Skip all Binance requests if blocked

               response.raise_for_status()
               price_data = response.json()

               # Try to get additional data, but fall back to basic if needed
               try:
                   # Get 24hr ticker statistics
                   ticker_url = "https://api.binance.com/api/v3/ticker/24hr"
                   ticker_response = requests.get(ticker_url, params={"symbol": f"{symbol}USDT"}, headers=headers, timeout=10)
                   ticker_data = ticker_response.json() if ticker_response.status_code == 200 else {}

                   # Get order book for bid/ask (optional)
                   depth_url = "https://api.binance.com/api/v3/depth"
                   depth_response = requests.get(depth_url, params={"symbol": f"{symbol}USDT", "limit": 5}, headers=headers, timeout=10)
                   depth_data = depth_response.json() if depth_response.status_code == 200 else {}

                   bid = float(depth_data['bids'][0][0]) if depth_data.get('bids') else 0
                   ask = float(depth_data['asks'][0][0]) if depth_data.get('asks') else 0
                   volume = float(ticker_data.get('volume', 0))

               except:
                   # Fall back to basic data
                   bid = ask = volume = 0

               crypto_record = {
                   'timestamp': datetime.utcnow().isoformat() + 'Z',
                   'symbol': symbol,
                   'exchange': 'binance',
                   'price': float(price_data['price']),
                   'volume': volume,
                   'bid': bid,
                   'ask': ask,
                   'spread': ask - bid if ask > 0 and bid > 0 else 0,
                   'market_cap': 0
               }

               binance_data.append(crypto_record)
               logger.info(f"📊 Fetched {symbol} data from Binance: ${crypto_record['price']:.2f}")

           except requests.exceptions.HTTPError as e:
               if e.response.status_code == 451:
                   logger.warning(f"⚠️  Binance API access restricted (error 451). This is common due to geographical restrictions.")
                   logger.info(f"💡 Tip: Try using a VPN or rely on other exchanges like Coinbase")
                   break  # Skip remaining Binance requests
               else:
                   logger.error(f"❌ HTTP error fetching {symbol} from Binance: {e}")
           except Exception as e:
               logger.error(f"❌ Failed to fetch {symbol} from Binance: {e}")

       return binance_data

   def fetch_coinbase_data(self, symbols: List[str]) -> List[Dict]:
       """
       Fetch real-time crypto data from Coinbase Pro API
       """
       coinbase_data = []

       headers = {
           'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
       }

       for symbol in symbols:
           try:
               # Get ticker data
               ticker_url = f"https://api.exchange.coinbase.com/products/{symbol}-USD/ticker"
               response = requests.get(ticker_url, headers=headers, timeout=10)
               response.raise_for_status()

               data = response.json()

               crypto_record = {
                   'timestamp': datetime.utcnow().isoformat() + 'Z',
                   'symbol': symbol,
                   'exchange': 'coinbase',
                   'price': float(data['price']),
                   'volume': float(data['volume']),
                   'bid': float(data['bid']),
                   'ask': float(data['ask']),
                   'spread': float(data['ask']) - float(data['bid']),
                   'market_cap': 0
               }

               coinbase_data.append(crypto_record)
               logger.info(f"📊 Fetched {symbol} data from Coinbase: ${crypto_record['price']:.2f}")

           except Exception as e:
               logger.error(f"❌ Failed to fetch {symbol} from Coinbase: {e}")

       return coinbase_data

   def fetch_coingecko_data(self, symbols: List[str]) -> List[Dict]:
       """
       Fetch crypto data from CoinGecko API (more reliable, no geo-restrictions)
       """
       coingecko_data = []

       # Symbol mapping for CoinGecko
       symbol_map = {
           'BTC': 'bitcoin',
           'ETH': 'ethereum',
           'ADA': 'cardano',
           'SOL': 'solana'
       }

       headers = {
           'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
       }

       try:
           # Get multiple coins in one request
           coin_ids = [symbol_map.get(symbol, symbol.lower()) for symbol in symbols if symbol in symbol_map]

           if not coin_ids:
               logger.warning("⚠️  No valid symbols for CoinGecko")
               return coingecko_data

           coins_param = ','.join(coin_ids)
           url = "https://api.coingecko.com/api/v3/simple/price"
           params = {
               'ids': coins_param,
               'vs_currencies': 'usd',
               'include_24hr_vol': 'true',
               'include_24hr_change': 'true',
               'include_market_cap': 'true'
           }

           response = requests.get(url, params=params, headers=headers, timeout=15)
           response.raise_for_status()

           data = response.json()

           for symbol in symbols:
               coin_id = symbol_map.get(symbol)
               if coin_id and coin_id in data:
                   coin_data = data[coin_id]

                   # Simulate bid/ask spread (CoinGecko doesn't provide this)
                   price = coin_data['usd']
                   spread = price * 0.001  # Assume 0.1% spread

                   crypto_record = {
                       'timestamp': datetime.utcnow().isoformat() + 'Z',
                       'symbol': symbol,
                       'exchange': 'coingecko',
                       'price': price,
                       'volume': coin_data.get('usd_24h_vol', 0),
                       'bid': price - spread/2,
                       'ask': price + spread/2,
                       'spread': spread,
                       'market_cap': coin_data.get('usd_market_cap', 0)
                   }

                   coingecko_data.append(crypto_record)
                   logger.info(f"📊 Fetched {symbol} data from CoinGecko: ${crypto_record['price']:.2f}")

       except Exception as e:
           logger.error(f"❌ Failed to fetch data from CoinGecko: {e}")

       return coingecko_data

   def fetch_kraken_data(self, symbols: List[str]) -> List[Dict]:
       """
       Fetch crypto data from Kraken API (another reliable alternative)
       """
       kraken_data = []

       # Symbol mapping for Kraken
       symbol_map = {
           'BTC': 'XXBTZUSD',
           'ETH': 'XETHZUSD',
           'ADA': 'ADAUSD',
           'SOL': 'SOLUSD'
       }

       headers = {
           'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
       }

       for symbol in symbols:
           if symbol not in symbol_map:
               continue

           try:
               kraken_symbol = symbol_map[symbol]
               url = "https://api.kraken.com/0/public/Ticker"
               params = {'pair': kraken_symbol}

               response = requests.get(url, params=params, headers=headers, timeout=10)
               response.raise_for_status()

               data = response.json()

               if data.get('error'):
                   logger.error(f"❌ Kraken API error for {symbol}: {data['error']}")
                   continue

               if 'result' in data and kraken_symbol in data['result']:
                   ticker = data['result'][kraken_symbol]

                   crypto_record = {
                       'timestamp': datetime.utcnow().isoformat() + 'Z',
                       'symbol': symbol,
                       'exchange': 'kraken',
                       'price': float(ticker['c'][0]),  # Last trade closed
                       'volume': float(ticker['v'][1]),  # Volume last 24 hours
                       'bid': float(ticker['b'][0]),  # Best bid price
                       'ask': float(ticker['a'][0]),  # Best ask price
                       'spread': float(ticker['a'][0]) - float(ticker['b'][0]),
                       'market_cap': 0
                   }

                   kraken_data.append(crypto_record)
                   logger.info(f"📊 Fetched {symbol} data from Kraken: ${crypto_record['price']:.2f}")

           except Exception as e:
               logger.error(f"❌ Failed to fetch {symbol} from Kraken: {e}")

       return kraken_data

   def ingest_crypto_data(self, crypto_data: List[Dict]) -> bool:
       """
       Ingest crypto data into QuestDB using REST API /exec endpoint
       """
       if not crypto_data:
           logger.warning("⚠️  No crypto data to ingest")
           return False

       # Build INSERT query
       values_list = []
       for record in crypto_data:
           values = (
               f"'{record['timestamp']}',"
               f"'{record['symbol']}',"
               f"'{record['exchange']}',"
               f"{record['price']},"
               f"{record['volume']},"
               f"{record['bid']},"
               f"{record['ask']},"
               f"{record['spread']},"
               f"{record['market_cap']}"
           )
           values_list.append(f"({values})")

       insert_sql = f"""
       INSERT INTO crypto_prices
       (timestamp, symbol, exchange, price, volume, bid, ask, spread, market_cap)
       VALUES {','.join(values_list)}
       """

       try:
           response = self.session.get(
               f"{self.questdb_url}/exec",
               params={"query": insert_sql}
           )
           response.raise_for_status()
           logger.info(f"✅ Successfully ingested {len(crypto_data)} crypto records")
           return True
       except Exception as e:
           logger.error(f"❌ Failed to ingest data: {e}")
           logger.error(f"Query: {insert_sql[:200]}...")
           return False

def demonstrate_crypto_pipeline():
   """
   Main demonstration function showing the complete crypto data pipeline
   """
   print("🚀 Starting QuestDB Crypto Data Pipeline Demo")
   print("=" * 60)

   # Initialize components
   ingestion = QuestDBCryptoIngestion()

   # Step 1: Create table schema
   print("\n📋 Step 1: Creating crypto_prices table...")
   if not ingestion.create_crypto_table():
       print("❌ Failed to create table. Make sure QuestDB is running on localhost:9000")
       return

   # Step 2: Fetch and ingest crypto data from multiple sources
   print("\n📊 Step 2: Fetching crypto data from multiple exchanges...")
   symbols = ["BTC", "ETH", "ADA", "SOL"]

   all_crypto_data = []

   # Try multiple data sources (more reliable)
   print("\n🔄 Trying Binance API...")
   binance_data = ingestion.fetch_binance_data(symbols)
   all_crypto_data.extend(binance_data)

   print("\n🔄 Trying Coinbase API...")
   coinbase_data = ingestion.fetch_coinbase_data(symbols)
   all_crypto_data.extend(coinbase_data)

   print("\n🔄 Trying CoinGecko API...")
   coingecko_data = ingestion.fetch_coingecko_data(symbols)
   all_crypto_data.extend(coingecko_data)

   print("\n🔄 Trying Kraken API...")
   kraken_data = ingestion.fetch_kraken_data(symbols)
   all_crypto_data.extend(kraken_data)

   # Summary of data collection
   print(f"\n📈 Data Collection Summary:")
   print(f"   • Binance: {len(binance_data)} records")
   print(f"   • Coinbase: {len(coinbase_data)} records")
   print(f"   • CoinGecko: {len(coingecko_data)} records")
   print(f"   • Kraken: {len(kraken_data)} records")
   print(f"   • Total: {len(all_crypto_data)} records")

   # Ingest the data
   if all_crypto_data:
       print(f"\n💾 Ingesting {len(all_crypto_data)} records into QuestDB...")
       ingestion.ingest_crypto_data(all_crypto_data)
   else:
       print("⚠️  No crypto data fetched from any source.")
       print("💡 This might be due to:")
       print("   - Network connectivity issues")
       print("   - API rate limiting or geographical restrictions")
       print("   - Temporary API downtime")
       print("\n🔧 Troubleshooting tips:")
       print("   - Try using a VPN if you're getting 451 errors")
       print("   - Check your internet connection")
       print("   - Wait a few minutes and try again")
       return

if __name__ == "__main__":
   demonstrate_crypto_pipeline()
