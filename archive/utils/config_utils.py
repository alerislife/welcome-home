"""
Configuration utilities for the ETL pipeline.

This module contains functions for loading and creating configuration files,
with support for environment variables for sensitive information.
"""

import configparser
import os
from datetime import datetime
import logging
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

logger = logging.getLogger(__name__)


def load_config(config_path):
    if not config_path:
        raise ValueError("Configuration file path cannot be empty")

    config = configparser.ConfigParser()

    # Read the config file
    files_read = config.read(config_path)
    if not files_read:
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    # Convert to dictionary for easier access
    config_dict = {}
    for section in config.sections():
        config_dict[section] = dict(config[section])

    # Validate required sections
    required_sections = ['Azure', 'Snowflake']
    for section in required_sections:
        if section not in config_dict:
            raise ValueError(f"Required configuration section '{section}' is missing")

    # Override with environment variables for sensitive information

    # Azure credentials - extract from connection string or use from env
    if 'AZURE_CONNECTION_STRING' in os.environ:
        config_dict['Azure']['connection_string'] = os.getenv('AZURE_CONNECTION_STRING')

    # API Credentials

    config_dict['WelcomeHome']['api_key'] = os.getenv('WELCOME_HOME_API_KEY')

    # Snowflake credentials
    if 'Snowflake' in config_dict:
        config_dict['Snowflake']['password'] = os.getenv('SNOWFLAKE_PASSWORD', config_dict['Snowflake'].get('password', ''))

    # Validate that required credentials are available
    if not config_dict['WelcomeHome']['api_key']:
        raise ValueError("Welcome Home API key not found in environment variables or config file. Please set WELCOME_HOME_API_KEY in .env file.")
    
    # Validate Snowflake credentials
    if 'Snowflake' in config_dict:
        snowflake_config = config_dict['Snowflake']
        required_snowflake_fields = ['user', 'password', 'account', 'warehouse', 'database', 'schema', 'stage_name']
        missing_fields = []
        
        for field in required_snowflake_fields:
            if not snowflake_config.get(field):
                missing_fields.append(field)
        
        if missing_fields:
            raise ValueError(f"Missing Snowflake configuration fields: {', '.join(missing_fields)}. Please check your config.ini file and ensure SNOWFLAKE_PASSWORD is set in .env file.")

    logger.info("Configuration loaded successfully")
    return config_dict
