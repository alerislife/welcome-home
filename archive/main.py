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
from typing import Dict

# Add the current directory to the Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from archive.utils.config_utils import load_config
from archive.utils.azure_utils import upload_to_azure_blob
from archive.utils.wh_api_utils import download_table_csv
from archive.utils.snowflake_utils import load_data_to_snowflake

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

# Define all tables to process
TABLES = [
    "Prospects",
    "Residents",
    "Activities",
    "DepositTransactions",
]


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


def api_download_and_upload(table_name: str, config: Dict) -> bool:
    """Download table data from API and upload to Azure Blob Storage.

    Args:
        table_name: Name of the table to process
        config: Configuration dictionary with API and Azure details

    Returns:
        bool: True if successful, False otherwise
    """
    logger.info(f"Processing {colorize_table_name(table_name)} - API Download and Blob Upload")

    # Create temporary directory for CSV files
    with tempfile.TemporaryDirectory() as temp_dir:
        logger.info(f"Using temporary directory: {temp_dir}")

        # Step 1: Download CSV from API
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

        # Step 2: Upload to Azure Blob Storage
        logger.info("=" * 80)
        logger.info("STEP 2: Upload CSV to blob storage from temporary location")
        logger.info("=" * 80)

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

    logger.info(f"API download and blob upload completed successfully for {colorize_table_name(table_name)}")
    return True


def snowflake_load(table_name: str, config: Dict) -> bool:
    """Load data from Azure Blob Storage to Snowflake.

    Args:
        table_name: Name of the table to load into Snowflake
        config: Configuration dictionary with Snowflake details

    Returns:
        bool: True if successful, False otherwise
    """
    logger.info(f"Processing {colorize_table_name(table_name)} - Snowflake Load")

    logger.info("=" * 80)
    logger.info(f"Loading data into Snowflake from stage for table: {table_name}")
    logger.info("=" * 80)

    # Get the SQL file path
    sql_file_path = Path(__file__).parent / "sql" / f"{camel_to_snake_case(table_name)}.sql"

    if not sql_file_path.exists():
        logger.warning(f"SQL file not found: {sql_file_path}")
        logger.info("To complete the pipeline, create the appropriate SQL file and execute it in Snowflake")
        return False

    logger.info(f"SQL file available at: {sql_file_path}")

    # Option to load data directly to Snowflake if configured
    if not config.get('Snowflake'):
        logger.warning("Snowflake configuration missing - cannot load data")
        return False

    try:
        # Create azure_config, it's no longer passed in
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
            return True
        else:
            logger.error(f"Failed to load data to Snowflake for table: {colorize_table_name(table_name)}")
            return False

    except Exception as e:
        logger.error(f"Snowflake load failed: {str(e)}")
        logger.info("Please execute the SQL file manually in Snowflake")
        return False


def main():
    """Main function to orchestrate the Welcome Home data export process."""
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

        # Process all tables
        successful_tables = []
        failed_tables = []
        
        for table_name in TABLES:
            logger.info("\n" + "=" * 120)
            logger.info(f"PROCESSING TABLE: {colorize_table_name(table_name)}")
            logger.info("=" * 120)
            
            try:
                # Step 1: API Download and Blob Upload
                upload_success = api_download_and_upload(table_name, config)

                # Step 2: Snowflake Load (if upload was successful)
                if upload_success:
                    load_success = snowflake_load(table_name, config)
                    if load_success:
                        successful_tables.append(table_name)
                    else:
                        failed_tables.append(table_name)
                else:
                    # If upload failed, the whole process for the table fails
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