# Import the libraries
import requests
import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

# Load the environment variables
load_dotenv(override=True)

#Retrieve API Key and Database Credentials
BITQUERY_API_KEY = os.getenv("BITQUERY_API_KEY")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

# PostgreSQL Connection
engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# Fetches transaction data from Bitquery V2 API.
def extract_data():
    query = """
    query {
      ethereum(network: bsc) {
        dexTrades(
          options: {desc: "block.height", limit: 10}
          exchangeName: {in: ["Pancake", "Pancake v2"]}
          date: {after: "2021-04-28"}
        ) {
          date {
            date
          }
          buyAmount
          buyAmountInAUD: buyAmount(in: AUD)
          buyCurrency {
            symbol
          }
          sellAmount
          sellAmountInAUD: sellAmount(in: AUD)
          sellCurrency {
            symbol
          }
          tradeAmount(in: AUD)
          transaction {
            hash
            gasValue
            gasPrice
            gas
          }
        }
      }
    }
    """
    
    headers = {
        "Authorization": f"Bearer {BITQUERY_API_KEY}",
        "Content-Type": "application/json"
    }
    
    response = requests.post("https://streaming.bitquery.io/graphql", json={"query": query}, headers=headers)

    print("Status Code:", response.status_code)
    if response.status_code != 200:
        print("Error fetching data:", response.text)
        return []

    response_json = response.json()

    # Error Handling
    if response_json.get('data') is None:
        print("API returned null data with errors:", response_json.get('errors'))
        return []
    
    if 'ethereum' in response_json['data'] and 'dexTrades' in response_json['data']['ethereum']:
        return response_json["data"]["ethereum"]["dexTrades"]
    else:
        print("Unexpected response structure:", response_json)
        return []

# Transforms raw API data into a structured DataFrame.
def transform_data(trades):
    if not trades:
        print("No transactions to transform.")
        return pd.DataFrame()

    data = []
    for trade in trades:
        try:
            data.append({
                "date": trade["date"]["date"],
                "buy_amount": trade["buyAmount"],
                "buy_amount_in_aud": trade["buyAmountInAUD"],
                "buy_currency": trade["buyCurrency"]["symbol"] if trade.get("buyCurrency") else None,
                "sell_amount": trade["sellAmount"],
                "sell_amount_in_aud": trade["sellAmountInAUD"],
                "sell_currency": trade["sellCurrency"]["symbol"] if trade.get("sellCurrency") else None,
                "trade_amount": trade["tradeAmount(in: AUD)"],
                "transaction_hash": trade["transaction"]["hash"],
                "gas_value": trade["transaction"]["gasValue"],
                "gas_price": trade["transaction"]["gasPrice"],
                "gas_used": trade["transaction"]["gas"]
            })
        except KeyError as e:
            print(f"Missing key {e} in trade:", trade)

    df = pd.DataFrame(data)
    print("Data transformed successfully!")
    return df

# Inserts transformed data into PostgreSQL database.
def load_data_to_db(df):
    if df.empty:
        print("No data to save.")
        return

    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        cursor = conn.cursor()

        # Insert data (Avoid duplicates using ON CONFLICT)
        for _, row in df.iterrows():
            cursor.execute("""
                INSERT INTO dex_trades (
                    date, buy_amount, buy_amount_in_aud, buy_currency, 
                    sell_amount, sell_amount_in_aud, sell_currency, 
                    trade_amount, transaction_hash, gas_value, gas_price, gas_used
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (transaction_hash) DO NOTHING;
            """, tuple(row))

        conn.commit()
        cursor.close()
        conn.close()
        print("Data successfully saved to PostgreSQL!")

    except Exception as e:
        print("Error saving to database:", e)

    
# Run the ETL process
trades = extract_data()
df = transform_data(trades)
load_data_to_db(df)