"""Welcome Home API utilities for downloading CSV data.

This module contains functions for downloading CSV files from the Welcome Home API
with support for paginated responses.
"""

import logging
import requests
import tempfile
import os
import re
from typing import Optional

logger = logging.getLogger(__name__)


def download_table_csv(table_name: str, api_key: str, temp_dir: str) -> Optional[str]:
    """
    Download CSV data for a specific table from the Welcome Home API.
    
    Handles paginated responses by following the Link header until all data is retrieved.
    
    Args:
        table_name (str): Name of the table to download
        api_key (str): API key for authentication
        temp_dir (str): Temporary directory to store the CSV file
    
    Returns:
        Optional[str]: Path to the downloaded CSV file, or None if failed
    """
    base_url = f"https://crm.welcomehomesoftware.com/api/exports/community/all/table/{table_name}"
    
    headers = {
        'Authorization': f'Bearer {api_key}',
        'Accept': 'text/csv'
    }
    
    # Create the CSV file path
    csv_file_path = os.path.join(temp_dir, f"{table_name}.csv")
    
    logger.info(f"Starting download for table: {table_name}")
    
    try:
        current_url = base_url
        page_count = 0
        total_records = 0
        
        with open(csv_file_path, 'w', newline='', encoding='utf-8') as csv_file:
            while current_url:
                page_count += 1
                logger.info(f"Downloading page {page_count} from: {current_url}")
                
                response = requests.get(current_url, headers=headers)
                response.raise_for_status()
                
                # Write the response content to the CSV file
                content = response.text
                
                # For the first page, write the entire content including headers
                # For subsequent pages, skip the header line
                if page_count == 1:
                    csv_file.write(content)
                    # Count lines (subtract 1 for header)
                    lines_in_page = len(content.strip().split('\n')) - 1
                else:
                    # Skip the first line (header) for subsequent pages
                    lines = content.strip().split('\n')
                    if len(lines) > 1:  # Make sure there's more than just the header
                        csv_file.write('\n' + '\n'.join(lines[1:]))
                        lines_in_page = len(lines) - 1
                    else:
                        lines_in_page = 0
                
                total_records += lines_in_page
                logger.info(f"Page {page_count}: {lines_in_page} records")
                
                # Check for next page in Link header
                current_url = _get_next_page_url(response.headers.get('Link'))
                
                if not current_url:
                    logger.info(f"No more pages. Download complete.")
                    break
        
        logger.info(f"Successfully downloaded {total_records} records for table '{table_name}' to {csv_file_path}")
        return csv_file_path
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Error downloading data for table '{table_name}': {str(e)}")
        if hasattr(e, 'response') and e.response is not None:
            logger.error(f"Response status: {e.response.status_code}")
            logger.error(f"Response body: {e.response.text}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error downloading table '{table_name}': {str(e)}")
        return None


def _get_next_page_url(link_header: Optional[str]) -> Optional[str]:
    """
    Extract the next page URL from the Link header.
    
    Args:
        link_header (str): Link header value from HTTP response
    
    Returns:
        Optional[str]: Next page URL if found, None otherwise
    """
    if not link_header:
        return None
    
    # Parse Link header format: <URL>; rel="next"
    # Example: <https://crm.welcomehomesoftware.com/api/path/to/endpoint?cursor=6eb0d8c2e74cedf3>; rel="next"
    match = re.search(r'<([^>]+)>\s*;\s*rel=["\']?next["\']?', link_header)
    
    if match:
        next_url = match.group(1)
        logger.debug(f"Found next page URL: {next_url}")
        return next_url
    
    logger.debug("No next page URL found in Link header")
    return None