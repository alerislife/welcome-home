import os
import re
import requests
import snowflake.connector
from datetime import datetime
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv
import logging
import pytz
import pandas as pd
from io import StringIO
import sys
import argparse

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

# --- Constants ---
# Snowflake connection details
SNOWFLAKE_ACCOUNT = "naa26543.east-us-2.azure"
SNOWFLAKE_USER = "svc_etl"
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_WAREHOUSE = "compute_wh"
SNOWFLAKE_DATABASE = "raw"
SNOWFLAKE_SCHEMA = "welcome_home_export"

# WelcomeHome API details
API_BASE_URL = "https://crm.welcomehomesoftware.com/api/exports/community/all/table"
API_TOKEN = os.getenv("WELCOME_HOME_API_KEY") # Assuming you have a token
TABLES_TO_PROCESS = [
    "Organizations",
    "Referrers",
    "Influencers",
    "Units",
    "HousingContracts",
    "DepositTransactions",
    "MarketingTouchpoints",
    "ServiceAgreements",
    "Traits",
    "Prospects",
    "Residents",
    "Activities",
]

def to_snake_case(name):
    """Converts a PascalCase string to snake_case."""
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()

def blue_text(text):
    """Returns text formatted in blue color for terminal output."""
    return f"\033[34m{text}\033[0m"

def green_text(text):
    """Returns text formatted in green color for terminal output."""
    return f"\033[32m{text}\033[0m"

def get_snowflake_connection():
    """Establishes a connection to Snowflake."""
    try:
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA
        )
        return conn
    except Exception as e:
        logging.error(f"Error connecting to Snowflake: {e}")
        return None

def fetch_all_ids_from_api(table_name, records_per_page=10000):
    """Fetches all record IDs from a given API table, handling pagination."""
    all_ids = []
    url = f"{API_BASE_URL}/{table_name}?limit={records_per_page}"
    headers = {
        "Authorization": f"Bearer {API_TOKEN}",
        "Accept": "application/json"
    }
    
    page_number = 1
    logging.info(f"Starting data fetch for table: {blue_text(table_name)}")
    
    while url:
        try:
            response = requests.get(url, headers=headers)
            
            response.raise_for_status()  # Raises an HTTPError for bad responses (4xx or 5xx)
            
            # Log response content for debugging
            response_text = response.text
            
            if not response_text.strip():
                logging.error(f"  - Empty response from API for {table_name}")
                return []
            
            # Parse CSV data
            try:
                df = pd.read_csv(StringIO(response_text))
                
                # Look for the {table_name}.id column
                snake_case_table = to_snake_case(table_name)
                expected_id_column = f"{snake_case_table}.id"
                
                if expected_id_column in df.columns:
                    id_column = expected_id_column
                    logging.info(f"  - Found expected ID column: {id_column}")
                else:
                    # Fallback to first column if expected column not found
                    id_column = df.columns[0]
                    logging.warning(f"  - Expected ID column '{expected_id_column}' not found. Using first column as fallback: {id_column}")
                
                # Extract IDs, filtering out null values
                ids = df[id_column].dropna().tolist()
                all_ids.extend(ids)
                logging.info(f"  - Page {page_number}: Fetched {len(ids)} records")
                page_number += 1
                
            except Exception as csv_error:
                logging.error(f"  - Error parsing CSV for {table_name}: {csv_error}")
                return []

            # Handle pagination
            if 'Link' in response.headers and 'rel="next"' in response.headers['Link']:
                url = response.headers['Link'].split(';')[0].strip('<>')
            else:
                url = None # No more pages

        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching data for {table_name}: {e}")
            return [] # Return empty list on error

    logging.info(f"Finished fetching for {blue_text(table_name)}. Total IDs: {len(all_ids)}")
    return all_ids

def create_table_and_load_data(connection, table_name, ids):
    """Creates a table in Snowflake and loads the data using pandas for optimal performance."""
    snowflake_table_name = to_snake_case(table_name)
    
    # Get current EST time
    est_tz = pytz.timezone('US/Eastern')
    load_timestamp = datetime.now(est_tz)

    try:
        # 1. Create or replace table structure
        cursor = connection.cursor()
        create_table_query = f"""
        CREATE OR REPLACE TABLE {snowflake_table_name} (
            id NUMBER,
            load_dts TIMESTAMP_NTZ
        );
        """
        cursor.execute(create_table_query)
        logging.info(f"Table '{blue_text(snowflake_table_name)}' created or replaced.")
        cursor.close()

        # 2. Load data using pandas DataFrame and write_pandas for optimal performance
        if ids:
            # Create pandas DataFrame with uppercase column names to match Snowflake identifiers
            # Convert datetime to string format for Snowflake TIMESTAMP_NTZ compatibility
            load_timestamp_str = load_timestamp.strftime('%Y-%m-%d %H:%M:%S')
            df = pd.DataFrame({
                'ID': ids,
                'LOAD_DTS': [load_timestamp_str] * len(ids)
            })
            
            # Use write_pandas to load data efficiently (replaces existing data)
            success, nchunks, nrows, _ = write_pandas(
                conn=connection,
                df=df,
                table_name=snowflake_table_name.upper(),  # Snowflake expects uppercase table names
                overwrite=True  # This replaces all existing data
            )
            
            if success:
                logging.info(f"{green_text('SUCCESSFULLY LOADED')} {nrows} records into '{blue_text(snowflake_table_name)}' using pandas (processed in {nchunks} chunks).")
            else:
                logging.error(f"Failed to load data into '{blue_text(snowflake_table_name)}' using pandas.")
        else:
            logging.info(f"{green_text('No new data to load')} for '{blue_text(snowflake_table_name)}'.")

    except Exception as e:
        logging.error(f"Error during Snowflake operation for table {blue_text(snowflake_table_name)}: {e}")
        raise  # Re-raise the exception to stop processing

def main(specific_tables=None):
    """Main function to orchestrate the data pipeline."""
    conn = get_snowflake_connection()
    if not conn:
        return

    # Valid table names
    valid_tables = ["Organizations", "Referrers", "Influencers", "Units", "HousingContracts", "DepositTransactions", "MarketingTouchpoints", "ServiceAgreements", "Traits", "Prospects", "Residents", "Activities"]

    try:
        # If specific tables are provided, process only those tables
        # Otherwise, process all tables in TABLES_TO_PROCESS
        if specific_tables:
            # Parse comma-separated table names and strip whitespace
            tables_to_run = [table.strip() for table in specific_tables.split(',')]
            
            # Validate all table names
            for table in tables_to_run:
                if table not in valid_tables:
                    logging.error(f"Invalid table name: '{table}'. Valid tables are: {', '.join(valid_tables)}")
                    sys.exit(1)
        else:
            tables_to_run = TABLES_TO_PROCESS
        
        logging.info(f"Processing {len(tables_to_run)} table(s): {', '.join(tables_to_run)}")
        
        for table in tables_to_run:
            logging.info(f"Processing table: {blue_text(table)}")
            ids = fetch_all_ids_from_api(table)
            if ids:
                create_table_and_load_data(conn, table, ids)  # Pass connection instead of cursor
            logging.info("---")
    finally:
        conn.close()
        logging.info("Snowflake connection closed.")

if __name__ == "__main__":
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description='Process Welcome Home data tables',
        epilog='Examples:\n'
               '  python main.py                    # Process all tables\n'
               '  python main.py Prospects          # Process single table\n'
               '  python main.py "Prospects,Activities,Units"  # Process multiple tables\n',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('tables', nargs='?', help='Specific table(s) to process. Can be a single table or comma-separated list (e.g., "Prospects,Activities")')
    args = parser.parse_args()
    
    if not SNOWFLAKE_PASSWORD:
        logging.error("Error: SNOWFLAKE_PASSWORD must be set in the .env file.")
        sys.exit(1)
    if not API_TOKEN:
        logging.error("Error: WELCOME_HOME_API_TOKEN must be set in the .env file.")
        sys.exit(1)
    
    # Run main function with optional table argument(s)
    if args.tables:
        main(args.tables)
    else:
        logging.info("Processing all tables")
        main()