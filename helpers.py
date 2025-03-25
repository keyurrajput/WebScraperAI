#util/helper.py

import os
import re
import json
import time
import random
import string
import logging
import urllib.parse
import pandas as pd
from typing import Dict, List, Any, Optional, Union, Tuple

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def generate_unique_id(prefix: str = '') -> str:
    """
    Generate a unique ID with an optional prefix
    
    Args:
        prefix: Optional prefix for the ID
        
    Returns:
        Unique ID string
    """
    timestamp = int(time.time() * 1000)
    random_str = ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))
    
    if prefix:
        return f"{prefix}_{timestamp}_{random_str}"
    else:
        return f"{timestamp}_{random_str}"

def clean_filename(filename: str) -> str:
    """
    Clean a filename by removing invalid characters
    
    Args:
        filename: Filename to clean
        
    Returns:
        Cleaned filename
    """
    # Remove invalid characters
    filename = re.sub(r'[\\/*?:"<>|]', '', filename)
    
    # Replace multiple spaces with a single space
    filename = re.sub(r'\s+', ' ', filename)
    
    # Trim to reasonable length
    if len(filename) > 100:
        name, ext = os.path.splitext(filename)
        filename = name[:96] + ext
    
    return filename.strip()

def extract_domain(url: str) -> str:
    """
    Extract the domain from a URL
    
    Args:
        url: URL to extract domain from
        
    Returns:
        Domain name
    """
    try:
        parsed_url = urllib.parse.urlparse(url)
        domain = parsed_url.netloc
        
        # Remove 'www.' if present
        if domain.startswith('www.'):
            domain = domain[4:]
        
        return domain
    except:
        return ''

def format_file_size(size_bytes: int) -> str:
    """
    Format file size in a human-readable format
    
    Args:
        size_bytes: File size in bytes
        
    Returns:
        Formatted file size string
    """
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.2f} KB"
    elif size_bytes < 1024 * 1024 * 1024:
        return f"{size_bytes / (1024 * 1024):.2f} MB"
    else:
        return f"{size_bytes / (1024 * 1024 * 1024):.2f} GB"

def load_file_to_df(file_path: str) -> Optional[pd.DataFrame]:
    """
    Load a file to a pandas DataFrame
    
    Args:
        file_path: Path to the file
        
    Returns:
        DataFrame or None if loading failed
    """
    try:
        file_ext = os.path.splitext(file_path)[1].lower()
        
        if file_ext == '.csv':
            return pd.read_csv(file_path)
        elif file_ext in ['.xlsx', '.xls']:
            return pd.read_excel(file_path)
        elif file_ext == '.json':
            return pd.read_json(file_path)
        else:
            logger.error(f"Unsupported file type: {file_ext}")
            return None
    except Exception as e:
        logger.error(f"Error loading file {file_path}: {e}")
        return None

def get_file_info(file_path: str) -> Dict[str, Any]:
    """
    Get information about a file
    
    Args:
        file_path: Path to the file
        
    Returns:
        Dictionary with file information
    """
    try:
        filename = os.path.basename(file_path)
        file_size = os.path.getsize(file_path)
        file_ext = os.path.splitext(filename)[1].lower()
        
        return {
            'filename': filename,
            'path': file_path,
            'size': file_size,
            'size_formatted': format_file_size(file_size),
            'extension': file_ext,
            'last_modified': os.path.getmtime(file_path)
        }
    except Exception as e:
        logger.error(f"Error getting file info for {file_path}: {e}")
        return {
            'filename': os.path.basename(file_path),
            'path': file_path,
            'error': str(e)
        }

def search_queries_to_urls(queries: List[str], search_engine: str = 'google') -> List[str]:
    """
    Convert search queries to search engine URLs
    
    Args:
        queries: List of search queries
        search_engine: Search engine to use ('google', 'bing', 'duckduckgo')
        
    Returns:
        List of search engine URLs
    """
    urls = []
    
    for query in queries:
        encoded_query = urllib.parse.quote_plus(query)
        
        if search_engine.lower() == 'google':
            urls.append(f"https://www.google.com/search?q={encoded_query}")
        elif search_engine.lower() == 'bing':
            urls.append(f"https://www.bing.com/search?q={encoded_query}")
        elif search_engine.lower() == 'duckduckgo':
            urls.append(f"https://duckduckgo.com/?q={encoded_query}")
        else:
            urls.append(f"https://www.google.com/search?q={encoded_query}")
    
    return urls

def estimate_task_complexity(task_info: Dict[str, Any]) -> Tuple[str, int]:
    """
    Estimate the complexity of a scraping task
    
    Args:
        task_info: Information about the scraping task
        
    Returns:
        Tuple of (complexity level, estimated time in seconds)
    """
    # Initialize complexity score
    score = 0
    
    # Check data type
    data_type = task_info.get('data_type', 'text').lower()
    if data_type == 'text':
        score += 1
    elif data_type in ['image', 'audio']:
        score += 3
    elif data_type == 'video':
        score += 4
    elif data_type == 'mixed':
        score += 5
    
    # Check number of sources
    sources = task_info.get('sources', [])
    score += min(len(sources), 5)
    
    # Check number of attributes
    attributes = task_info.get('attributes', [])
    score += min(len(attributes), 5)
    
    # Check filters
    filters = task_info.get('filters', {})
    score += min(len(filters), 3)
    
    # Determine complexity level
    if score <= 5:
        complexity = 'Low'
        estimated_time = 30  # 30 seconds
    elif score <= 10:
        complexity = 'Medium'
        estimated_time = 120  # 2 minutes
    elif score <= 15:
        complexity = 'High'
        estimated_time = 300  # 5 minutes
    else:
        complexity = 'Very High'
        estimated_time = 600  # 10 minutes
    
    return complexity, estimated_time

def create_empty_directories(base_dir: str, dirs: List[str]) -> None:
    """
    Create empty directories
    
    Args:
        base_dir: Base directory
        dirs: List of directory names to create
    """
    for dir_name in dirs:
        dir_path = os.path.join(base_dir, dir_name)
        os.makedirs(dir_path, exist_ok=True)
        
        # Create a .gitkeep file to track empty directories in git
        gitkeep_path = os.path.join(dir_path, '.gitkeep')
        with open(gitkeep_path, 'w') as f:
            pass
