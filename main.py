#!/usr/bin/env python3
"""
Welcome Home Data Export Pipeline

This script orchestrates the process of:
1. Query API endpoint and download CSV to temporary location
2. Upload CSV to blob storage from temporary location
3. Use SQL to load table into Snowflake from stage

Usage:
    python main.py
"""

import logging
import os
import sys
import tempfile
from pathlib import Path
from typing import Dict, List

# Add the current directory to the Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from utils.config_utils import load_config
from utils.azure_utils import upload_to_azure_blob
from utils.wh_api_utils import download_table_csv
from utils.snowflake_utils import load_data_to_snowflake


# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

# ANSI color codes for terminal output
GREEN = '\033[92m'
RESET = '\033[0m'

def colorize_table_name(table_name: str) -> str:
    """Add green color to table name for terminal output"""
    return f"{GREEN}{table_name}{RESET}"


def camel_to_snake_case(name: str) -> str:
    """Convert CamelCase to snake_case"""
    import re
    # Insert underscore before uppercase letters that follow lowercase letters or digits
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    # Insert underscore before uppercase letters that follow lowercase letters
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


# Define all tables to process
TABLES = [
    # "Organizations",
    # "Referrers", 
    "Prospects",
    "Residents",
    # "Influencers",
    "Activities",
    # "Units",
    # "HousingContracts",
    "DepositTransactions",
    # "MarketingTouchpoints",
    # "ServiceAgreements",
    # "Traits"
]


def process_table(table_name: str, config: Dict, api_download: bool = True, blob_upload: bool = True, snowflake_load: bool = True) -> bool:
    """
    Process a table following the 3-step pipeline:
    1. Query API endpoint and download CSV to temporary location
    2. Upload CSV to blob storage from temporary location  
    3. Use SQL to load table into Snowflake from stage
    
    Args:
        table_name (str): Name of the table to process
        config (dict): Configuration dictionary
        api_download (bool): Whether to run step 1 (API download)
        blob_upload (bool): Whether to run step 2 (blob upload)
        snowflake_load (bool): Whether to run step 3 (Snowflake load)
    
    Returns:
        bool: True if successful, False otherwise
    """
    steps_to_run = []
    if api_download:
        steps_to_run.append("API Download")
    if blob_upload:
        steps_to_run.append("Blob Upload")
    if snowflake_load:
        steps_to_run.append("Snowflake Load")
    
    logger.info(f"Processing {colorize_table_name(table_name)} table - Steps: {', '.join(steps_to_run)}")
    
    csv_file_path = None
    azure_config = None
    
    # Create temporary directory for CSV files (only if we need to download)
    if api_download or blob_upload:
        with tempfile.TemporaryDirectory() as temp_dir:
            logger.info(f"Using temporary directory: {temp_dir}")
            
            if api_download:
                ############## Step 1: Query API endpoint and download CSV to temporary location  ##############
                logger.info("=" * 80)
                logger.info("STEP 1: Query API endpoint and download CSV to temporary location")
                logger.info("=" * 80)
                
                csv_file_path = download_table_csv(
                    table_name=table_name,
                    api_key=config['WelcomeHome']['api_key'],
                    temp_dir=temp_dir
                )
                
                if not csv_file_path:
                    logger.error(f"Failed to download CSV from API for table: {colorize_table_name(table_name)}")
                    return False
                
                # Verify file exists and has content
                if not os.path.exists(csv_file_path):
                    logger.error(f"CSV file does not exist: {csv_file_path}")
                    return False
                
                file_size = os.path.getsize(csv_file_path)
                logger.info(f"Downloaded and processed CSV file size: {file_size} bytes")
                
                if file_size == 0:
                    logger.warning(f"CSV file is empty for table: {colorize_table_name(table_name)}")
                    return False
            
            if blob_upload:
                ################ Step 2: Upload CSV to blob storage from temporary location   ##############
                logger.info("=" * 80)
                logger.info("STEP 2: Upload CSV to blob storage from temporary location")
                logger.info("=" * 80)
                
                if not csv_file_path and api_download:
                    logger.error("No CSV file available for blob upload")
                    return False
                elif not csv_file_path:
                    logger.error("Cannot upload to blob without first downloading CSV. Enable api_download=True.")
                    return False
                
                azure_config = {
                    'connection_string': config['Azure']['connection_string'],
                    'container_name': config['Azure']['container_name'],
                    'blob_name': f"{table_name.lower()}.csv"
                }
                
                success = upload_to_azure_blob(
                    azure_config=azure_config,
                    local_file=csv_file_path
                )
                
                if not success:
                    logger.error(f"Failed to upload CSV to blob storage for table: {colorize_table_name(table_name)}")
                    return False
    
    if snowflake_load:
        ################ Step 3: Use SQL to load table into Snowflake from stage   ##############
        logger.info("=" * 80)
        logger.info(f"STEP 3: Use SQL in sql/{camel_to_snake_case(table_name)}.sql to load table into Snowflake from stage")
        logger.info("=" * 80)
        
        sql_file_path = Path(__file__).parent / "sql" / f"{camel_to_snake_case(table_name)}.sql"
        
        if not sql_file_path.exists():
            logger.warning(f"SQL file not found: {sql_file_path}")
            logger.info("To complete the pipeline, create the appropriate SQL file and execute it in Snowflake")
            # If SQL file is missing, consider it a failure
            return False
        else:
            logger.info(f"SQL file available at: {sql_file_path}")
            
            # Option to load data directly to Snowflake if configured
            if config.get('Snowflake'):
                try:
                    # Create azure_config if not already set (for Snowflake-only runs)
                    if azure_config is None:
                        azure_config = {
                            'connection_string': config['Azure']['connection_string'],
                            'container_name': config['Azure']['container_name'],
                            'blob_name': f"{table_name.lower()}.csv"
                        }
                    
                    snowflake_config = {
                        'user': config['Snowflake']['user'],
                        'password': config['Snowflake']['password'],
                        'account': config['Snowflake']['account'],
                        'warehouse': config['Snowflake']['warehouse'],
                        'database': config['Snowflake']['database'],
                        'schema': config['Snowflake']['schema'],
                        'table_name': table_name.lower(),
                        'stage_name': config['Snowflake']['stage_name']
                    }
                    
                    load_success = load_data_to_snowflake(
                        snowflake_config=snowflake_config,
                        azure_config=azure_config,
                        file_path=str(sql_file_path)
                    )

                    if load_success:
                        logger.info(f"Data loaded successfully into snowflake table: {colorize_table_name(table_name)}")
                    else:
                        logger.error(f"Failed to auto-load data to Snowflake for table: {colorize_table_name(table_name)}")
                        return False
                        
                except Exception as e:
                    logger.error(f"Auto-load to Snowflake failed: {str(e)}")
                    logger.info("Please execute the SQL file manually in Snowflake")
                    return False
            else:
                logger.info("To complete the pipeline, execute the SQL file in Snowflake to load the data")
                # If Snowflake config is missing but we expect auto-loading, consider it a failure
                logger.warning("Snowflake configuration missing - cannot auto-load data")
                return False
        
        logger.info(f"Successfully processed table: {colorize_table_name(table_name)}")
        return True



def main():
    """
    Main function to orchestrate the Welcome Home data export process.
    Processes all configured tables following the 3-step pipeline:
    1. Query API endpoint and download CSV to temporary location
    2. Upload CSV to blob storage from temporary location
    3. Use SQL to load table into Snowflake from stage
    """
    logger.info("=" * 100)
    logger.info("WELCOME HOME DATA EXPORT PIPELINE STARTING")
    logger.info("=" * 100)
    
    try:
        # Load configuration
        config_path = Path(__file__).parent / "config.ini"
        config = load_config(str(config_path))
        
        # Validate required configuration
        if not config.get('WelcomeHome', {}).get('api_key'):
            logger.error("Welcome Home API key not configured. Please set WELCOME_HOME_API_KEY in .env file.")
            return 1
        
        if not config.get('Azure', {}).get('connection_string'):
            logger.error("Azure connection string not configured. Please set AZURE_CONNECTION_STRING in .env file.")
            return 1

        if not config.get('Snowflake'):
            logger.error("Snowflake configuration not found. Please check your config.ini file.")
            return 1
        
        # Process all tables following 3-step pipeline
        successful_tables = []
        failed_tables = []
        
        for table_name in TABLES:
            logger.info("\n" + "=" * 120)
            logger.info(f"PROCESSING TABLE: {colorize_table_name(table_name)}")
            logger.info("=" * 120)
            
            try:
                success = process_table(table_name, config)
                if success:
                    successful_tables.append(table_name)
                else:
                    failed_tables.append(table_name)
            except Exception as e:
                logger.error(f"Error processing table {colorize_table_name(table_name)}: {str(e)}")
                failed_tables.append(table_name)
        
        # Summary
        logger.info("\n" + "=" * 100)
        logger.info("PIPELINE COMPLETED")
        logger.info("=" * 100)
        
        if successful_tables:
            logger.info(f"Successfully processed {len(successful_tables)} tables: {', '.join(successful_tables)}")
        
        if failed_tables:
            logger.error(f"Failed to process {len(failed_tables)} tables: {', '.join(failed_tables)}")
        
        if not failed_tables:
            logger.info("All tables processed successfully!")
            return 0
        else:
            logger.error(f"Pipeline completed with {len(failed_tables)} failures.")
            return 1
            
    except Exception as e:
        logger.error(f"Pipeline failed with error: {str(e)}")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)