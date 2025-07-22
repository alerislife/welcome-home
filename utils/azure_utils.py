"""
Azure Blob Storage utilities for the ETL pipeline.

This module contains functions for uploading files to Azure Blob Storage.
"""

import logging
from azure.storage.blob import BlobServiceClient

logger = logging.getLogger(__name__)


def upload_to_azure_blob(azure_config, local_file):
    """
    Upload a file to Azure Blob Storage.

    Args:
        azure_config (dict): Azure Blob Storage configuration dictionary
        local_file (str): Path to the local file

    Returns:
        bool: True if successful, False otherwise
    """
    logger.info(f"Uploading file to Azure Blob Storage: {azure_config['blob_name']}")
    try:
        # Create a blob service client
        blob_service_client = BlobServiceClient.from_connection_string(
            azure_config['connection_string']
        )
        # Get container client
        container_client = blob_service_client.get_container_client(
            azure_config['container_name']
        )
        # Create a blob client
        blob_client = container_client.get_blob_client(azure_config['blob_name'])
        # Upload the file
        with open(local_file, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)

        logger.info(f"File uploaded successfully to: {azure_config['blob_name']}")
        return True

    except Exception as e:
        logger.error(f"Error uploading file to Azure Blob Storage: {str(e)}")
        return False