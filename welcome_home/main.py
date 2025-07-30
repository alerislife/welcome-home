import os
import requests
import snowflake.connector
from datetime import datetime
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv
import logging
import pytz

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
SNOWFLAKE_SCHEMA = "welcome_home"

# WelcomeHome API details
API_BASE_URL = "https://crm.welcomehomesoftware.com/api"
API_TOKEN = os.getenv("WELCOME_HOME_API_KEY")

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

def fetch_prospect_ids_from_activities(records_per_page=10000):
    """Fetches prospect IDs from activities endpoint, handling pagination."""
    prospect_ids = set()
    url = f"{API_BASE_URL}/activities"
    headers = {
        "Authorization": f"Bearer {API_TOKEN}",
        "Accept": "application/json"
    }
    
    page_number = 1
    logging.info(f"Starting prospect ID extraction from {blue_text('activities')} endpoint")
    
    params = {"limit": records_per_page}
    
    while url:
        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            
            activities = response.json()
            
            if not activities:
                logging.info(f"No more activities found on page {page_number}")
                break
                
            logging.info(f"Processing {len(activities)} activities from page {page_number}")
            
            # Extract prospect IDs from activities with record_type = 'Prospect'
            for activity in activities:
                if activity.get('record_type') == 'Prospect' and activity.get('record_id'):
                    prospect_ids.add(activity.get('record_id'))
            
            logging.info(f"Found {len(prospect_ids)} unique prospect IDs so far")
            
            # Check for next page using Link header
            link_header = response.headers.get('Link')
            if link_header and 'rel="next"' in link_header:
                # Parse the Link header to get next URL
                import re
                match = re.search(r'<([^>]+)>;\s*rel="next"', link_header)
                if match:
                    url = match.group(1)
                    params = None  # URL already contains pagination params
                    page_number += 1
                else:
                    url = None
            else:
                url = None
                
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching activities on page {page_number}: {e}")
            break
        except Exception as e:
            logging.error(f"Unexpected error processing activities on page {page_number}: {e}")
            break
    
    logging.info(f"Completed prospect ID extraction. Found {green_text(len(prospect_ids))} unique prospect IDs")
    return list(prospect_ids)

def create_table_and_load_data(connection, prospect_ids):
    """Creates the prospects table in Snowflake and loads the data."""
    table_name = "prospects"
    
    try:
        cursor = connection.cursor()
        
        # Drop table if exists and create new one
        drop_sql = f"DROP TABLE IF EXISTS {table_name}"
        cursor.execute(drop_sql)
        logging.info(f"Dropped existing table {blue_text(table_name)} if it existed")
        
        # Create table with id and load_dts columns
        create_sql = f"""
        CREATE TABLE {table_name} (
            id INTEGER,
            load_dts TIMESTAMP_NTZ
        )
        """
        cursor.execute(create_sql)
        logging.info(f"Created table {blue_text(table_name)}")
        
        # Prepare data for loading
        current_time = datetime.now(pytz.timezone('America/New_York')).strftime('%Y-%m-%d %H:%M:%S')
        
        data_records = []
        for prospect_id in prospect_ids:
            data_records.append({
                'id': prospect_id,
                'load_dts': current_time
            })
        
        if data_records:
            # Convert to DataFrame for efficient loading
            df = pd.DataFrame(data_records)
            
            # Use pandas to write data to Snowflake
            success, nchunks, nrows, _ = write_pandas(
                conn=connection,
                df=df,
                table_name=table_name.upper(),
                database=SNOWFLAKE_DATABASE,
                schema=SNOWFLAKE_SCHEMA
            )
            
            if success:
                logging.info(f"Successfully loaded {green_text(nrows)} prospect records into {blue_text(table_name)}")
            else:
                logging.error(f"Failed to load data into {table_name}")
        else:
            logging.warning(f"No prospect IDs found to load into {table_name}")
            
        cursor.close()
        
    except Exception as e:
        logging.error(f"Error creating table or loading data: {e}")
        raise

def main():
    """Main function to orchestrate the data pipeline."""
    logging.info("Starting Welcome Home prospects data pipeline")
    
    # Validate required environment variables
    if not API_TOKEN:
        logging.error("WELCOME_HOME_API_KEY environment variable is not set")
        return
        
    if not SNOWFLAKE_PASSWORD:
        logging.error("SNOWFLAKE_PASSWORD environment variable is not set")
        return
    
    # Get Snowflake connection
    connection = get_snowflake_connection()
    if not connection:
        logging.error("Failed to establish Snowflake connection")
        return
    
    try:
        # Fetch prospect IDs from activities
        prospect_ids = fetch_prospect_ids_from_activities()
        
        if prospect_ids:
            # Create table and load data
            create_table_and_load_data(connection, prospect_ids)
            logging.info(f"Pipeline completed successfully! Loaded {green_text(len(prospect_ids))} prospect records")
        else:
            logging.warning("No prospect IDs found from activities")
            
    except Exception as e:
        logging.error(f"Pipeline failed: {e}")
    finally:
        connection.close()
        logging.info("Snowflake connection closed")

if __name__ == "__main__":
    main()
