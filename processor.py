# core/processor.py

import os
import re
import json
import hashlib
import pandas as pd
import numpy as np
from typing import List, Dict, Any, Optional, Union
from PIL import Image
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DataProcessor:
    """Class for processing and transforming scraped data"""
    
    def __init__(self, output_dir: str):
        """
        Initialize the data processor
        
        Args:
            output_dir: Directory to save processed data
        """
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
    
    def process_data(self, data: List[Dict[str, Any]], task_info: Dict[str, Any]) -> pd.DataFrame:
        """
        Process scraped data based on task information
        
        Args:
            data: List of dictionaries with scraped data
            task_info: Information about the scraping task
            
        Returns:
            Processed DataFrame
        """
        if not data:
            return pd.DataFrame()
        
        # Convert to DataFrame
        df = pd.DataFrame(data)
        
        # Apply filters if specified
        if 'filters' in task_info and task_info['filters']:
            df = self._apply_filters(df, task_info['filters'])
        
        # Handle missing values
        df = self._handle_missing_values(df)
        
        # Clean and transform data
        df = self._clean_data(df)
        
        return df
    
    def _apply_filters(self, df: pd.DataFrame, filters: Dict[str, Any]) -> pd.DataFrame:
        """
        Apply filters to the DataFrame
        
        Args:
            df: DataFrame to filter
            filters: Dictionary of filters to apply
            
        Returns:
            Filtered DataFrame
        """
        filtered_df = df.copy()
        
        for column, filter_value in filters.items():
            if column not in filtered_df.columns:
                continue
                
            if isinstance(filter_value, dict):
                # Handle range filters
                if 'min' in filter_value and filter_value['min'] is not None:
                    filtered_df = filtered_df[filtered_df[column] >= filter_value['min']]
                
                if 'max' in filter_value and filter_value['max'] is not None:
                    filtered_df = filtered_df[filtered_df[column] <= filter_value['max']]
                    
                # Handle inclusion/exclusion filters
                if 'include' in filter_value and filter_value['include']:
                    filtered_df = filtered_df[filtered_df[column].isin(filter_value['include'])]
                
                if 'exclude' in filter_value and filter_value['exclude']:
                    filtered_df = filtered_df[~filtered_df[column].isin(filter_value['exclude'])]
            else:
                # Handle simple equality filter
                filtered_df = filtered_df[filtered_df[column] == filter_value]
        
        return filtered_df
    
    def _handle_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Handle missing values in the DataFrame
        
        Args:
            df: DataFrame with missing values
            
        Returns:
            DataFrame with handled missing values
        """
        # For numeric columns, fill missing values with the mean
        numeric_cols = df.select_dtypes(include=['number']).columns
        for col in numeric_cols:
            df[col] = df[col].fillna(df[col].mean())
        
        # For categorical columns, fill missing values with the mode
        categorical_cols = df.select_dtypes(include=['object', 'category']).columns
        for col in categorical_cols:
            if df[col].dropna().empty:
                df[col] = df[col].fillna("Unknown")
            else:
                df[col] = df[col].fillna(df[col].mode()[0])
        
        # For any remaining columns, fill with an appropriate placeholder
        df = df.fillna("N/A")
        
        return df
    
    def _clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and transform the DataFrame
        
        Args:
            df: DataFrame to clean
            
        Returns:
            Cleaned DataFrame
        """
        cleaned_df = df.copy()
        
        # Clean column names
        cleaned_df.columns = [self._clean_column_name(col) for col in cleaned_df.columns]
        
        # Clean text data
        for col in cleaned_df.select_dtypes(include=['object']).columns:
            cleaned_df[col] = cleaned_df[col].apply(lambda x: self._clean_text(x) if isinstance(x, str) else x)
        
        # Convert data types where appropriate
        cleaned_df = self._convert_data_types(cleaned_df)
        
        return cleaned_df
    
    def _clean_column_name(self, name: str) -> str:
        """
        Clean a column name
        
        Args:
            name: Column name to clean
            
        Returns:
            Cleaned column name
        """
        # Convert to lowercase
        name = name.lower()
        
        # Replace spaces and special characters with underscores
        name = re.sub(r'[^\w\s]', '', name)
        name = re.sub(r'\s+', '_', name)
        
        # Remove leading/trailing underscores
        name = name.strip('_')
        
        return name
    
    def _clean_text(self, text: str) -> str:
        """
        Clean a text value
        
        Args:
            text: Text to clean
            
        Returns:
            Cleaned text
        """
        # Remove leading/trailing whitespace
        text = text.strip()
        
        # Replace multiple spaces with a single space
        text = re.sub(r'\s+', ' ', text)
        
        # Remove HTML tags
        text = re.sub(r'<[^>]+>', '', text)
        
        return text
    
    def _convert_data_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Convert data types in the DataFrame
        
        Args:
            df: DataFrame to convert
            
        Returns:
            DataFrame with converted data types
        """
        converted_df = df.copy()
        
        for col in converted_df.columns:
            # Try to convert to numeric
            if converted_df[col].dtype == 'object':
                try:
                    # Check if column contains numbers with commas
                    if converted_df[col].str.contains(',').any():
                        converted_df[col] = converted_df[col].str.replace(',', '').astype(float)
                    else:
                        converted_df[col] = pd.to_numeric(converted_df[col], errors='ignore')
                except:
                    pass
                
                # Try to convert to datetime
                try:
                    if converted_df[col].dtype == 'object':
                        converted_df[col] = pd.to_datetime(converted_df[col], errors='ignore')
                except:
                    pass
        
        return converted_df
    
    def save_data(self, df: pd.DataFrame, output_format: str, filename: Optional[str] = None) -> str:
        """
        Save processed data to a file
        
        Args:
            df: DataFrame to save
            output_format: Format to save as (csv, excel, json)
            filename: Optional filename (without extension)
            
        Returns:
            Path to the saved file
        """
        if filename:
            base_filename = filename
        else:
            # Generate a filename based on timestamp
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            base_filename = f"dataset_{timestamp}"
        
        if output_format.lower() == 'csv':
            file_path = os.path.join(self.output_dir, f"{base_filename}.csv")
            df.to_csv(file_path, index=False)
        
        elif output_format.lower() == 'excel' or output_format.lower() == 'xlsx':
            file_path = os.path.join(self.output_dir, f"{base_filename}.xlsx")
            df.to_excel(file_path, index=False)
        
        elif output_format.lower() == 'json':
            file_path = os.path.join(self.output_dir, f"{base_filename}.json")
            df.to_json(file_path, orient='records', indent=4)
        
        else:
            # Default to CSV
            file_path = os.path.join(self.output_dir, f"{base_filename}.csv")
            df.to_csv(file_path, index=False)
        
        logger.info(f"Data saved to {file_path}")
        return file_path
    
    def process_media_files(self, file_paths: List[str], task_info: Dict[str, Any]) -> pd.DataFrame:
        """
        Process media files and create a metadata DataFrame
        
        Args:
            file_paths: List of paths to media files
            task_info: Information about the scraping task
            
        Returns:
            DataFrame with media metadata
        """
        if not file_paths:
            return pd.DataFrame()
        
        # Create metadata for each file
        metadata = []
        
        for file_path in file_paths:
            file_info = {
                'filename': os.path.basename(file_path),
                'path': file_path,
                'size_kb': round(os.path.getsize(file_path) / 1024, 2),
                'extension': os.path.splitext(file_path)[1].lower(),
                'file_type': self._get_file_type(file_path)
            }
            
            # Add image-specific metadata
            if file_info['file_type'] == 'image':
                try:
                    img = Image.open(file_path)
                    file_info['width'] = img.width
                    file_info['height'] = img.height
                    file_info['format'] = img.format
                    file_info['mode'] = img.mode
                except Exception as e:
                    logger.error(f"Error processing image {file_path}: {e}")
            
            metadata.append(file_info)
        
        # Create DataFrame
        df = pd.DataFrame(metadata)
        
        # Add topic and source information
        if 'topic' in task_info:
            df['topic'] = task_info['topic']
        
        return df
    
    def _get_file_type(self, file_path: str) -> str:
        """
        Get the type of a file
        
        Args:
            file_path: Path to the file
            
        Returns:
            File type (image, video, audio, document, other)
        """
        extension = os.path.splitext(file_path)[1].lower()
        
        image_extensions = ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp', '.svg', '.tiff']
        video_extensions = ['.mp4', '.avi', '.mov', '.wmv', '.flv', '.mkv', '.webm']
        audio_extensions = ['.mp3', '.wav', '.ogg', '.aac', '.flac', '.m4a']
        document_extensions = ['.pdf', '.doc', '.docx', '.txt', '.rtf', '.odt', '.ppt', '.pptx']
        
        if extension in image_extensions:
            return 'image'
        elif extension in video_extensions:
            return 'video'
        elif extension in audio_extensions:
            return 'audio'
        elif extension in document_extensions:
            return 'document'
        else:
            return 'other'
