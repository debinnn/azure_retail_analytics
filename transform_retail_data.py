import os
import pandas as pd
from azure.storage.blob import BlobServiceClient
from sqlalchemy import create_engine
import urllib
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configuration from environment variables
STORAGE_CONNECTION_STRING = os.getenv("STORAGE_CONNECTION_STRING")
CONTAINER_NAME = os.getenv("CONTAINER_NAME")

DB_SERVER = os.getenv("DB_SERVER")
DB_DATABASE = os.getenv("DB_DATABASE")
DB_USERNAME = os.getenv("DB_USERNAME")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_DRIVER = os.getenv("DB_DRIVER")

# 1. EXTRACT: Functions to get data from Azure Data Lake

def get_data_from_blob(blob_name):
    """Connects to Azure Blob Storage and reads a file into a pandas DataFrame."""
    from io import StringIO 
    try:
        blob_service_client = BlobServiceClient.from_connection_string(STORAGE_CONNECTION_STRING)
        blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=blob_name)
        
        downloader = blob_client.download_blob(max_concurrency=1, encoding='UTF-8')
        blob_text = downloader.readall()
        
        # Check file type and read accordingly
        if blob_name.endswith('.csv'):
            return pd.read_csv(StringIO(blob_text))
        elif blob_name.endswith('.json'):
            return pd.read_json(StringIO(blob_text))
        else:
            print(f"Unsupported file format for {blob_name}")
            return None

    except Exception as e:
        print(f"Error reading blob {blob_name}: {e}")
        return None
    
# 2. TRANSFORM: Main data processing logic

def transform_and_model():
    """Main function to run the ETL process."""
    print("Starting data extraction...")
    
    # --- E: Extract Data from all sources ---
    df_trans = get_data_from_blob("transactions/2024/1/15/transactions_20111209.csv")
    df_prod = get_data_from_blob("products/2024/1/15/product_catalog.json")
    df_cust = get_data_from_blob("customers/2024/1/15/customer_data.json")
    
    if df_trans is None or df_prod is None or df_cust is None:
        print("Aborting due to data extraction failure.")
        return

    print("Data extraction complete.")
    
    # --- T: Transform Data ---
    print("Starting data transformation...")
    
    # Clean transactions data
    df_trans.dropna(subset=['CustomerID', 'Description'], inplace=True)
    df_trans['CustomerID'] = df_trans['CustomerID'].astype(int)
    df_trans['InvoiceDate'] = pd.to_datetime(df_trans['InvoiceDate'])
    df_trans = df_trans[df_trans['Quantity'] > 0]
    df_trans = df_trans[df_trans['UnitPrice'] > 0]
    df_trans['Revenue'] = df_trans['Quantity'] * df_trans['UnitPrice']

    # --- Create Dimensional Model ---
    
    # DimDate
    print("Creating DimDate...")
    df_date = df_trans[['InvoiceDate']].drop_duplicates()
    df_date['date_key'] = df_date['InvoiceDate'].dt.strftime('%Y%m%d').astype(int)
    df_date['year'] = df_date['InvoiceDate'].dt.year
    df_date['month'] = df_date['InvoiceDate'].dt.month
    df_date['day'] = df_date['InvoiceDate'].dt.day
    df_date['quarter'] = df_date['InvoiceDate'].dt.quarter
    df_date['weekday'] = df_date['InvoiceDate'].dt.day_name()
    dim_date = df_date[['date_key', 'InvoiceDate', 'year', 'month', 'day', 'quarter', 'weekday']].rename(columns={'InvoiceDate': 'full_date'})

    # DimCustomer
    print("Creating DimCustomer...")
    df_cust_base = df_trans[['CustomerID', 'Country']].drop_duplicates().copy() # Added .copy()
    dim_customer = pd.merge(df_cust_base, df_cust, on='CustomerID', how='left')
    dim_customer.rename(columns={'CustomerID': 'customer_key'}, inplace=True)

    # DimProduct
    print("Creating DimProduct...")
    dim_product = df_trans[['StockCode', 'Description']].drop_duplicates().copy() # Added .copy()
    dim_product = pd.merge(dim_product, df_prod, on='StockCode', how='left')
    dim_product.rename(columns={'StockCode': 'product_key'}, inplace=True)
    dim_product['Category'] = dim_product['Category'].fillna('Unknown') # Switched to safer method
    dim_product['Brand'] = dim_product['Brand'].fillna('Unknown')       # Switched to safer method

    # --- Create FactSales (NEW, MORE RELIABLE LOGIC) ---
    print("Creating FactSales...")
    
    # Add surrogate keys directly to the transactions DataFrame
    df_trans['date_key'] = df_trans['InvoiceDate'].dt.strftime('%Y%m%d').astype(int)
    
    # Merge with dimensions to get the other keys
    merged_df = pd.merge(df_trans, dim_customer, left_on='CustomerID', right_on='customer_key')
    merged_df = pd.merge(merged_df, dim_product, left_on='StockCode', right_on='product_key')

     # Select final columns for the fact table from the fully merged DataFrame
    fact_sales = merged_df[[
        'InvoiceNo',
        'date_key',
        'customer_key',
        'product_key',
        'Quantity',
        'UnitPrice_x',  # Use the correct suffixed column
        'Revenue'
    ]]

    # Rename the column back to 'UnitPrice' for a clean final table
    fact_sales = fact_sales.rename(columns={'UnitPrice_x': 'UnitPrice'})
    
    print("Data transformation and modeling complete.")
    
    return dim_date, dim_customer, dim_product, fact_sales

# 3. LOAD: Function to upload data to Azure SQL

def load_to_sql(dataframes, table_names):
    """Connects to Azure SQL and loads DataFrames into specified tables."""
    print("Connecting to Azure SQL Database...")
    try:
        # Create a connection string for SQLAlchemy
        params = urllib.parse.quote_plus(
            f"DRIVER={DB_DRIVER};"
            f"SERVER=tcp:{DB_SERVER},1433;"
            f"DATABASE={DB_DATABASE};"
            f"UID={DB_USERNAME};"
            f"PWD={DB_PASSWORD};"
            f"Encrypt=yes;"
            f"TrustServerCertificate=no;"
            "Connection Timeout=30;"
        )
        conn_str = f"mssql+pyodbc:///?odbc_connect={params}"
        engine = create_engine(conn_str)
        
        print("Connection successful. Loading data...")
        
        for df, table_name in zip(dataframes, table_names):
            print(f"Loading data into {table_name}...")
            # Using 'replace' for simplicity in this portfolio project.
            # In a real-world scenario, you'd use 'append' with incremental logic.
            df.to_sql(table_name, engine, if_exists='replace', index=False, chunksize=1000)
            print(f"Successfully loaded {len(df)} rows into {table_name}.")

    except Exception as e:
        print(f"Failed to load data to SQL. Error: {e}")

# Main execution block

if __name__ == "__main__":
    dim_date, dim_customer, dim_product, fact_sales = transform_and_model()
    
    # Prepare for loading
    dataframes_to_load = [dim_date, dim_customer, dim_product, fact_sales]
    table_names_to_load = ["DimDate", "DimCustomer", "DimProduct", "FactSales"]
    
    load_to_sql(dataframes_to_load, table_names_to_load)
    
    print("\n--- DATA PIPELINE RUN COMPLETE ---")