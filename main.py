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

# Fetch data from Bitquery API
def extract_data():
    query = """
    query {
      EVM(dataset: combined, network: eth) {
        Transactions(limit: {count: 10}, orderBy: {descending: Block_Time}) {
          Transaction {
            Hash
            From
            To
            Value
            Gas
            GasPrice
          }
          Block {
            Number
            Time
          }
        }
      }
    }
    """
    
    headers = {
        "Authorization": f"Bearer {BITQUERY_API_KEY}", 
        "Content-Type": "application/json"
    }
    
    # Note the different endpoint URL
    response = requests.post("https://streaming.bitquery.io/graphql", json={"query": query}, headers=headers)
    
    print("Status Code:", response.status_code)
    print("Response Text:", response.text[:500])
    
    if response.status_code == 200:
        response_json = response.json()
        # Add a check for null data
        if response_json.get('data') is None:
            print("API returned null data with errors:", response_json.get('errors'))
            return []
        
        if 'EVM' in response_json['data'] and 'Transactions' in response_json['data']['EVM']:
            return response_json["data"]["EVM"]["Transactions"]
        else:
            print("Unexpected response structure:", response_json)
            return []
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
    df["value"] = df["value"].astype(float) / 1e18  
    df.drop(columns=["from", "to", "block"], inplace=True)
    print("Data transformed successfully!")
    print(df)
    return df

# Function to load data into PostgreSQL
def load_data_to_postgres(df):
    df.to_sql("ethereum_transactions", engine, if_exists="append", index=False)
    print("Data loaded successfully into PostgreSQL!")

    
# Run ETL pipeline
def run_etl():
    print("Running ETL Pipeline...")
    transactions = extract_data()
    if transactions:
        df = transform_data(transactions)
        load_data_to_postgres(df)
    print("ETL Pipeline Completed!")

# Execute pipeline
if __name__ == "__main__":
    run_etl()