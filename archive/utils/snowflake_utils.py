"""
Snowflake utilities for the ETL pipeline.

This module contains functions for loading data into Snowflake.
"""

import logging
import os
import snowflake.connector
import pandas as pd
from typing import Dict, List

logger = logging.getLogger(__name__)


def load_data_to_snowflake(snowflake_config, azure_config, file_path):
    """
    Load data from Azure Blob Storage into Snowflake.

    Args:
        snowflake_config (dict): Snowflake configuration dictionary
        azure_config (dict): Azure Blob Storage configuration dictionary

    Returns:
        bool: True if successful, False otherwise
    """
    logger.info("Connecting to Snowflake")
    try:
        # Connect to Snowflake
        conn = snowflake.connector.connect(
            user=snowflake_config['user'],
            password=snowflake_config['password'],
            account=snowflake_config['account'],
            warehouse=snowflake_config['warehouse'],
            database=snowflake_config['database'],
            schema=snowflake_config['schema']
        )

        cursor = conn.cursor()

        # Read SQL from file and format it with configuration parameters
        sql = read_sql_file(file_path)

        # Replace the placeholders with actual values from the configuration
        sql = sql.format(
            database=snowflake_config['database'],
            schema=snowflake_config['schema'],
            stage_name=snowflake_config['stage_name'],
            blob_name=azure_config['blob_name']
        )

        # Split the SQL into individual statements
        sql_statements = sql.split(';')

        # Execute each statement individually
        for stmt in sql_statements:
            # Skip empty statements
            if stmt.strip():
                logger.info(f"Executing SQL statement: {stmt.strip()[:100]}...")  # Log first 100 chars for brevity
                cursor.execute(stmt)

        # Close the connection
        conn.close()

        logger.info(f"Data loaded successfully into the table")
        return True

    except Exception as e:
        logger.error(f"Error loading data into Snowflake: {str(e)}")
        return False


def read_sql_file(file_path):
    """
    Read SQL from a file.

    Args:
        file_path (str): Path to the SQL file

    Returns:
        str: SQL content

    Raises:
        FileNotFoundError: If the SQL file doesn't exist
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"SQL file not found: {file_path}")

    with open(file_path, 'r') as f:
        sql = f.read()

    return sql
