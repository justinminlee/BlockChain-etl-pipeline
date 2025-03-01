# Import the libraries
import requests
import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

# Load the environment variables
load_dotenv()

# Retrieve API Key and Database Credentials
BITQUERY_API_KEY = os.getenv("BITQUERY_API_KEY")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

# PostgreSQL Connection
engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# Fetch data from Bitquery API
def extract_data():
    query = """
    {
      ethereum(network: ethereum) {
        transactions(options: {limit: 100, desc: "block.timestamp.time"}) {
          hash
          from { address }
          to { address }
          value
          gas
          gas_price
          block { timestamp { time } height }
        }
      }
    }
    """
    headers = {"X-API-KEY": BITQUERY_API_KEY, "Content-Type": "application/json"}
    response = requests.post("https://graphql.bitquery.io/", json={"query": query}, headers=headers)
    
    if response.status_code == 200:
        return response.json()["data"]["ethereum"]["transactions"]
    else:
        print("Error fetching data:", response.text)
        return []

# Transform data
def transform_data(transactions):
    df = pd.DataFrame(transactions)
    df.rename(columns={"hash": "tx_hash"}, inplace=True)
    df["from_address"] = df["from"].apply(lambda x: x["address"] if x else None)
    df["to_address"] = df["to"].apply(lambda x: x["address"] if x else None)
    df["timestamp"] = df["block"].apply(lambda x: x["timestamp"]["time"] if x else None)
    df["block_height"] = df["block"].apply(lambda x: x["height"] if x else None)
    df["value"] = df["value"].astype(float) / 1e18  # Convert Wei to ETH
    df.drop(columns=["from", "to", "block"], inplace=True)
    return 

