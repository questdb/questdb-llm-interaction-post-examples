#!/usr/bin/env python3
"""
Claude Desktop Crypto Data Interaction Example
==============================================

This script demonstrates how Claude Desktop can interact with QuestDB crypto data
using natural language queries via the REST API. This simulates the actual
interaction patterns you'd see when using Claude Desktop with your crypto database.

Run this after the main ingestion script to see AI-powered crypto analysis in action.
"""

import requests
import pandas as pd
from datetime import datetime
from typing import Dict
import matplotlib.pyplot as plt

class ClaudeCryptoAnalyst:
   """
   Simulates Claude Desktop's interaction with QuestDB crypto data
   """

   def __init__(self, questdb_host: str = "localhost", questdb_port: int = 9000):
       self.questdb_url = f"http://{questdb_host}:{questdb_port}"

   def ask_claude(self, user_question: str) -> str:
       """
       Simulates asking Claude Desktop a natural language question about crypto data
       """
       print(f"\n🤖 Claude Desktop Processing: '{user_question}'")
       print("🧠 Thinking... translating to SQL and executing...")

       # This simulates Claude's thought process
       query_analysis = self._analyze_user_intent(user_question)
       sql_query = self._generate_sql(query_analysis)
       results = self._execute_questdb_query(sql_query)
       response = self._format_human_response(results, user_question)

       return response

   def _analyze_user_intent(self, question: str) -> Dict:
       """
       Simulates Claude's analysis of user intent (what Claude does internally)
       """
       question_lower = question.lower()

       analysis = {
           "intent": "unknown",
           "entities": [],
           "time_range": "latest",
           "metrics": [],
           "comparison": False
       }

       # Intent classification
       if any(word in question_lower for word in ["price", "cost", "value"]):
           analysis["intent"] = "price_analysis"
       elif any(word in question_lower for word in ["volume", "trading", "activity"]):
           analysis["intent"] = "volume_analysis"
       elif any(word in question_lower for word in ["arbitrage", "difference", "spread", "opportunity"]):
           analysis["intent"] = "arbitrage_analysis"
       elif any(word in question_lower for word in ["trend", "change", "movement", "performance"]):
           analysis["intent"] = "trend_analysis"
       elif any(word in question_lower for word in ["compare", "vs", "versus", "between"]):
           analysis["intent"] = "comparison"
           analysis["comparison"] = True

       # Extract crypto symbols
       crypto_symbols = ["btc", "bitcoin", "eth", "ethereum", "ada", "cardano", "sol", "solana"]
       for symbol in crypto_symbols:
           if symbol in question_lower:
               if symbol in ["btc", "bitcoin"]:
                   analysis["entities"].append("BTC")
               elif symbol in ["eth", "ethereum"]:
                   analysis["entities"].append("ETH")
               elif symbol in ["ada", "cardano"]:
                   analysis["entities"].append("ADA")
               elif symbol in ["sol", "solana"]:
                   analysis["entities"].append("SOL")

       # Extract time ranges
       if any(word in question_lower for word in ["hour", "last hour", "recent"]):
           analysis["time_range"] = "1_hour"
       elif any(word in question_lower for word in ["day", "today", "24 hour"]):
           analysis["time_range"] = "1_day"
       elif any(word in question_lower for word in ["week", "7 day"]):
           analysis["time_range"] = "1_week"

       return analysis

   def _generate_sql(self, analysis: Dict) -> str:
       """
       Simulates Claude generating SQL based on intent analysis
       """
       time_conditions = {
           "latest": "timestamp > dateadd('m', -15, now())",
           "1_hour": "timestamp > dateadd('h', -1, now())",
           "1_day": "timestamp > dateadd('d', -1, now())",
           "1_week": "timestamp > dateadd('d', -7, now())"
       }

       time_filter = time_conditions.get(analysis["time_range"], time_conditions["latest"])

       # Symbol filter
       symbol_filter = ""
       if analysis["entities"]:
           symbols = "', '".join(analysis["entities"])
           symbol_filter = f" AND symbol IN ('{symbols}')"

       if analysis["intent"] == "price_analysis":
           if analysis["comparison"]:
               # Build WHERE clause for LATEST ON queries
               where_clause = ""
               if analysis["entities"]:
                   symbols = "', '".join(analysis["entities"])
                   where_clause = f"WHERE symbol IN ('{symbols}')"

               return f"""
               SELECT
                   symbol,
                   exchange,
                   price,
                   timestamp
               FROM crypto_prices
               LATEST ON timestamp PARTITION BY symbol, exchange
               {where_clause}
               ORDER BY symbol, exchange
               """
           else:
               return f"""
               SELECT
                   symbol,
                   exchange,
                   price,
                   timestamp
               FROM crypto_prices
               WHERE {time_filter} {symbol_filter}
               ORDER BY timestamp DESC
               LIMIT 20
               """

       elif analysis["intent"] == "volume_analysis":
           return f"""
           SELECT
               symbol,
               exchange,
               sum(volume) as total_volume,
               avg(volume) as avg_volume,
               count(*) as data_points
           FROM crypto_prices
           WHERE {time_filter} {symbol_filter}
           GROUP BY symbol, exchange
           ORDER BY total_volume DESC
           """

       elif analysis["intent"] == "arbitrage_analysis":
           # Build the symbol filter for the outer query instead
           symbol_where = ""
           if analysis["entities"]:
               symbols = "', '".join(analysis["entities"])
               symbol_where = f"WHERE lp1.symbol IN ('{symbols}')"

           return f"""
           WITH latest_by_exchange AS (
               SELECT symbol, exchange, price
               FROM crypto_prices
               LATEST ON timestamp PARTITION BY symbol, exchange
           )
           SELECT
               lp1.symbol,
               lp1.exchange as exchange1,
               lp1.price as price1,
               lp2.exchange as exchange2,
               lp2.price as price2,
               abs(lp1.price - lp2.price) as price_diff,
               (abs(lp1.price - lp2.price) / ((lp1.price + lp2.price) / 2) * 100) as arbitrage_pct
           FROM latest_by_exchange lp1
           JOIN latest_by_exchange lp2 ON lp1.symbol = lp2.symbol AND lp1.exchange < lp2.exchange
           {symbol_where}
           AND abs(lp1.price - lp2.price) / ((lp1.price + lp2.price) / 2) * 100 > 0.1
           ORDER BY arbitrage_pct DESC
           """

       elif analysis["intent"] == "trend_analysis":
           return f"""
           SELECT
               symbol,
               exchange,
               first(price) as earliest_price,
               last(price) as latest_price,
               (last(price) - first(price)) / first(price) * 100 as price_change_pct,
               min(price) as min_price,
               max(price) as max_price,
               count(*) as data_points
           FROM crypto_prices
           WHERE {time_filter} {symbol_filter}
           GROUP BY symbol, exchange
           ORDER BY price_change_pct DESC
           """

       else:
           # Default query
           return f"""
           SELECT symbol, exchange, price, volume, timestamp
           FROM crypto_prices
           WHERE {time_filter} {symbol_filter}
           ORDER BY timestamp DESC
           LIMIT 10
           """

   def _execute_questdb_query(self, sql_query: str) -> Dict:
       """
       Execute the SQL query against QuestDB
       """
       try:
           response = requests.get(
               f"{self.questdb_url}/exec",
               params={"query": sql_query}
           )
           response.raise_for_status()
           return response.json()
       except Exception as e:
           return {"error": str(e)}

   def _format_human_response(self, results: Dict, original_question: str) -> str:
       """
       Simulates Claude formatting the results into a human-readable response
       """
       if "error" in results:
           return f"❌ I encountered an error while analyzing your crypto data: {results['error']}"

       if not results.get("dataset") or len(results["dataset"]) == 0:
           return "📊 I couldn't find any matching crypto data for your query. The database might be empty or the time range might be too restrictive."

       data = results["dataset"]
       columns = [col["name"] for col in results.get("columns", [])]

       # Format response based on data content
       response_parts = []
       response_parts.append(f"📈 Based on your question '{original_question}', here's what I found:")
       response_parts.append(f"\n🔍 Found {len(data)} records with {len(columns)} data points each.")

       # Analyze the results and provide insights
       if "price" in columns:
           prices = [row[columns.index("price")] for row in data if row[columns.index("price")] is not None]
           if prices:
               avg_price = sum(prices) / len(prices)
               min_price = min(prices)
               max_price = max(prices)
               response_parts.append(f"\n💰 Price Analysis:")
               response_parts.append(f"   • Average Price: ${avg_price:,.2f}")
               response_parts.append(f"   • Price Range: ${min_price:,.2f} - ${max_price:,.2f}")

       if "arbitrage_pct" in columns:
           arb_opportunities = [row for row in data if row[columns.index("arbitrage_pct")] > 1.0]
           response_parts.append(f"\n🎯 Arbitrage Opportunities:")
           if arb_opportunities:
               response_parts.append(f"   • Found {len(arb_opportunities)} opportunities > 1%")
               for i, opp in enumerate(arb_opportunities[:3]):
                   symbol_idx = columns.index("symbol")
                   ex1_idx = columns.index("exchange1")
                   ex2_idx = columns.index("exchange2")
                   arb_idx = columns.index("arbitrage_pct")
                   response_parts.append(f"   • {opp[symbol_idx]}: {opp[arb_idx]:.2f}% between {opp[ex1_idx]} and {opp[ex2_idx]}")
           else:
               response_parts.append("   • No significant arbitrage opportunities found (>1%)")

       if "total_volume" in columns:
           volumes = [row[columns.index("total_volume")] for row in data]
           total_volume = sum(volumes)
           response_parts.append(f"\n📊 Volume Analysis:")
           response_parts.append(f"   • Total Volume: {total_volume:,.2f}")
           response_parts.append(f"   • Average per Exchange: {total_volume/len(volumes):,.2f}")

       # Show sample data
       response_parts.append(f"\n📋 Sample Data:")
       for i, row in enumerate(data[:3]):
           row_str = " | ".join([f"{col}: {val}" for col, val in zip(columns, row)])
           response_parts.append(f"   {i+1}. {row_str}")

       if len(data) > 3:
           response_parts.append(f"   ... and {len(data) - 3} more records")

       return "\n".join(response_parts)

   def create_visualization(self, question: str) -> str:
       """
       Create a simple visualization based on the question (simulates Claude's visualization capabilities)
       """
       print(f"\n📊 Creating visualization for: '{question}'")

       # Get price data for visualization
       query = """
       SELECT
           symbol,
           exchange,
           price,
           timestamp
       FROM crypto_prices
       WHERE timestamp > dateadd('h', -6, now())
       ORDER BY timestamp
       """

       try:
           response = requests.get(f"{self.questdb_url}/exec", params={"query": query})
           data = response.json()

           if not data.get("dataset"):
               return "❌ No data available for visualization"

           # Convert to DataFrame for easy plotting
           df = pd.DataFrame(data["dataset"], columns=[col["name"] for col in data["columns"]])
           df['timestamp'] = pd.to_datetime(df['timestamp'])

           # Create a simple price chart
           plt.figure(figsize=(12, 6))

           for symbol in df['symbol'].unique():
               for exchange in df['exchange'].unique():
                   subset = df[(df['symbol'] == symbol) & (df['exchange'] == exchange)]
                   if not subset.empty:
                       plt.plot(subset['timestamp'], subset['price'],
                               marker='o', label=f"{symbol} ({exchange})", linewidth=2)

           plt.title("Crypto Prices Over Time", fontsize=16, fontweight='bold')
           plt.xlabel("Time")
           plt.ylabel("Price (USD)")
           plt.legend()
           plt.grid(True, alpha=0.3)
           plt.xticks(rotation=45)
           plt.tight_layout()

           # Save the plot
           filename = f"crypto_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
           plt.savefig(filename, dpi=300, bbox_inches='tight')
           plt.close()

           return f"📈 Visualization saved as {filename}"

       except Exception as e:
           return f"❌ Failed to create visualization: {e}"

def demonstrate_crypto_ai_interaction():
   """
   Demonstrate natural language interaction with crypto data
   """
   print("🤖 Claude Desktop Crypto Analysis Demo")
   print("=" * 50)

   claude = ClaudeCryptoAnalyst()

   # Simulate user questions that Claude Desktop would handle
   user_questions = [
       "What are the latest Bitcoin prices?",
       "Show me arbitrage opportunities between exchanges",
       "How much trading volume do we have for ETH?",
       "Compare prices between Binance and Coinbase",
       "What's the trend for Solana in the last hour?",
       "Which crypto has the highest volume today?",
       "Find me the best arbitrage opportunities for ADA",
       "What's the average spread across all exchanges?"
   ]

   print("\n🎯 Simulating natural language crypto analysis...")
   print("(This is what you'd see when using Claude Desktop with your QuestDB crypto data)")

   for question in user_questions:
       print("\n" + "="*80)
       print(f"👤 User: {question}")

       # Get Claude's response
       response = claude.ask_claude(question)
       print(f"\n🤖 Claude Desktop Response:")
       print(response)

       # Add a small delay to simulate thinking time
       import time
       time.sleep(1)

   # Demonstrate visualization capability
   print("\n" + "="*80)
   print("📊 VISUALIZATION DEMO")
   viz_result = claude.create_visualization("Show me price trends")
   print(viz_result)

   print("\n🎉 Claude Desktop Crypto Analysis Demo Complete!")
   print("\n💡 Key Benefits Demonstrated:")
   print("   ✅ Natural language queries → SQL translation")
   print("   ✅ Intelligent data analysis and insights")
   print("   ✅ Human-readable responses with context")
   print("   ✅ Automatic visualization generation")
   print("   ✅ Real-time arbitrage opportunity detection")

   print("\n🚀 Next Steps for Production:")
   print("   • Set up Claude Desktop with MCP server for persistent connections")
   print("   • Implement real-time data streaming from crypto exchanges")
   print("   • Add alert system for significant price movements")
   print("   • Create custom dashboards with Grafana integration")
   print("   • Implement portfolio tracking and P&L analysis")

def simulate_advanced_crypto_scenarios():
   """
   Demonstrate more advanced crypto analysis scenarios
   """
   print("\n🔬 ADVANCED CRYPTO ANALYSIS SCENARIOS")
   print("=" * 60)

   claude = ClaudeCryptoAnalyst()

   advanced_scenarios = [
       {
           "scenario": "Flash Crash Detection",
           "question": "Did any crypto drop more than 5% in the last hour?",
           "sql_override": """
           SELECT
               symbol,
               exchange,
               first(price) as start_price,
               last(price) as end_price,
               (last(price) - first(price)) / first(price) * 100 as change_pct
           FROM crypto_prices
           WHERE timestamp > dateadd('h', -1, now())
           GROUP BY symbol, exchange
           HAVING (last(price) - first(price)) / first(price) * 100 < -5
           ORDER BY change_pct ASC
           """
       },
       {
           "scenario": "High Frequency Trading Analysis",
           "question": "Show me price volatility by 5-minute intervals",
           "sql_override": """
           SELECT
               symbol,
               exchange,
               date_trunc('minute', timestamp) as time_bucket,
               min(price) as low,
               max(price) as high,
               first(price) as open,
               last(price) as close,
               (max(price) - min(price)) / avg(price) * 100 as volatility_pct
           FROM crypto_prices
           WHERE timestamp > dateadd('h', -2, now())
           GROUP BY symbol, exchange, date_trunc('minute', timestamp)
           ORDER BY time_bucket DESC
           LIMIT 50
           """
       },
       {
           "scenario": "Liquidity Analysis",
           "question": "Which exchanges have the tightest spreads?",
           "sql_override": """
           SELECT
               exchange,
               symbol,
               avg(spread) as avg_spread,
               avg(spread/price * 100) as avg_spread_pct,
               min(spread/price * 100) as min_spread_pct,
               max(spread/price * 100) as max_spread_pct,
               count(*) as observations
           FROM crypto_prices
           WHERE timestamp > dateadd('h', -24, now())
             AND spread > 0
             AND price > 0
           GROUP BY exchange, symbol
           HAVING count(*) > 5
           ORDER BY avg_spread_pct ASC
           """
       }
   ]

   for scenario in advanced_scenarios:
       print(f"\n🎯 {scenario['scenario']}")
       print("-" * 40)
       print(f"Question: {scenario['question']}")

       # Execute custom SQL for advanced scenarios
       try:
           response = requests.get(
               f"{claude.questdb_url}/exec",
               params={"query": scenario['sql_override']}
           )
           results = response.json()

           if results.get("dataset"):
               print(f"✅ Found {len(results['dataset'])} results")

               # Show insights based on scenario
               if scenario['scenario'] == "Flash Crash Detection":
                   crashes = results['dataset']
                   if crashes:
                       print("🚨 FLASH CRASHES DETECTED:")
                       for crash in crashes[:3]:
                           print(f"   • {crash[0]} on {crash[1]}: {crash[4]:.2f}% drop")
                   else:
                       print("✅ No significant crashes detected")

               elif scenario['scenario'] == "High Frequency Trading Analysis":
                   volatility_data = results['dataset']
                   if volatility_data:
                       avg_vol = sum(row[7] for row in volatility_data if row[7]) / len(volatility_data)
                       print(f"📊 Average 5-min volatility: {avg_vol:.2f}%")
                       high_vol = [row for row in volatility_data if row[7] and row[7] > 2.0]
                       print(f"⚡ High volatility periods (>2%): {len(high_vol)}")

               elif scenario['scenario'] == "Liquidity Analysis":
                   liquidity_data = results['dataset']
                   if liquidity_data:
                       best_exchange = min(liquidity_data, key=lambda x: x[3])
                       print(f"🏆 Tightest spreads: {best_exchange[0]} ({best_exchange[3]:.3f}%)")
                       print(f"📈 Total exchange-symbol pairs analyzed: {len(liquidity_data)}")
           else:
               print("⚠️ No data found for this analysis")

       except Exception as e:
           print(f"❌ Analysis failed: {e}")

def create_crypto_dashboard_export():
   """
   Create dashboard-ready data exports
   """
   print("\n📊 DASHBOARD DATA EXPORT")
   print("=" * 40)

   claude = ClaudeCryptoAnalyst()

   dashboard_queries = {
       "price_summary": """
       WITH latest_prices AS (
           SELECT symbol, exchange, price, volume, timestamp,
                  ROW_NUMBER() OVER (PARTITION BY symbol, exchange ORDER BY timestamp DESC) as rn
           FROM crypto_prices
       )
       SELECT
           symbol,
           exchange,
           price,
           volume,
           timestamp
       FROM latest_prices
       WHERE rn = 1
       ORDER BY symbol, exchange
       """,

       "hourly_ohlc": """
       SELECT
           symbol,
           exchange,
           date_trunc('hour', timestamp) as hour,
           first(price) as open,
           max(price) as high,
           min(price) as low,
           last(price) as close,
           sum(volume) as volume
       FROM crypto_prices
       WHERE timestamp > dateadd('d', -1, now())
       SAMPLE BY 1h
       """,

       "market_overview": """
       SELECT
           symbol,
           count(DISTINCT exchange) as exchange_count,
           avg(price) as avg_price,
           sum(volume) as total_volume,
           (max(price) - min(price)) / avg(price) * 100 as price_range_pct
       FROM crypto_prices
       WHERE timestamp > dateadd('h', -24, now())
       GROUP BY symbol
       ORDER BY total_volume DESC
       """
   }

   for dashboard_name, query in dashboard_queries.items():
       print(f"\n📋 Exporting {dashboard_name.replace('_', ' ').title()}...")

       try:
           # Get CSV format for dashboard consumption
           response = requests.get(
               f"{claude.questdb_url}/exp",
               params={"query": query}
           )

           if response.status_code == 200:
               csv_data = response.text
               filename = f"crypto_{dashboard_name}_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"

               with open(filename, 'w') as f:
                   f.write(csv_data)

               lines = len(csv_data.split('\n')) - 1  # Subtract header
               print(f"✅ Exported {lines} records to {filename}")

               # Show preview
               preview_lines = csv_data.split('\n')[:4]  # Header + 3 data rows
               print("🔍 Preview:")
               for line in preview_lines:
                   if line.strip():
                       print(f"   {line}")

           else:
               print(f"❌ Export failed for {dashboard_name}")

       except Exception as e:
           print(f"❌ Export error: {e}")

if __name__ == "__main__":
   print("🚀 Starting Comprehensive Crypto Analysis Demo")

   # Main demo
   demonstrate_crypto_ai_interaction()

   # Advanced scenarios
   simulate_advanced_crypto_scenarios()

   # Dashboard exports
   create_crypto_dashboard_export()

   print("\n" + "="*80)
   print("🎯 SUMMARY: Claude Desktop + QuestDB Crypto Analysis")
   print("="*80)
   print("✅ Natural Language Processing: Convert questions to SQL")
   print("✅ Real-time Analysis: Live crypto market insights")
   print("✅ Arbitrage Detection: Cross-exchange opportunity identification")
   print("✅ Volatility Monitoring: Flash crash and spike detection")
   print("✅ Data Export: Dashboard-ready CSV generation")
   print("✅ Visualization: Automated chart creation")

   print(f"\n🔗 QuestDB Web Console: http://localhost:9000")
   print("💡 Pro Tip: Use the web console to explore your crypto data visually!")

   print("\n🚀 Ready for Production:")
   print("   • Scale with real-time data feeds")
   print("   • Add alerting for trading opportunities")
   print("   • Implement portfolio tracking")
   print("   • Connect to trading APIs for automated execution")
