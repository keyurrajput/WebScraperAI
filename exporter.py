# core/exporter.py

import os
import json
import zipfile
import shutil
import pandas as pd
import logging
from typing import List, Dict, Any, Optional, Union
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DatasetExporter:
    """Class for exporting datasets in various formats"""
    
    def __init__(self, output_dir: str):
        """
        Initialize the dataset exporter
        
        Args:
            output_dir: Directory to save exported datasets
        """
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
    
    def export_dataset(self, 
                       data_file: str, 
                       media_files: Optional[List[str]] = None, 
                       metadata: Optional[Dict[str, Any]] = None,
                       output_format: str = 'zip') -> str:
        """
        Export a dataset with data and optional media files
        
        Args:
            data_file: Path to the data file
            media_files: Optional list of media file paths
            metadata: Optional metadata about the dataset
            output_format: Format to export as (zip, directory)
            
        Returns:
            Path to the exported dataset
        """
        # Create a timestamp for the dataset name
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Create a dataset directory
        dataset_name = f"dataset_{timestamp}"
        dataset_dir = os.path.join(self.output_dir, dataset_name)
        os.makedirs(dataset_dir, exist_ok=True)
        
        # Copy the data file
        data_filename = os.path.basename(data_file)
        shutil.copy2(data_file, os.path.join(dataset_dir, data_filename))
        
        # Copy media files if provided
        if media_files:
            media_dir = os.path.join(dataset_dir, 'media')
            os.makedirs(media_dir, exist_ok=True)
            
            for media_file in media_files:
                if os.path.exists(media_file):
                    shutil.copy2(media_file, os.path.join(media_dir, os.path.basename(media_file)))
        
        # Add metadata if provided
        if metadata:
            metadata_file = os.path.join(dataset_dir, 'metadata.json')
            with open(metadata_file, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, indent=4)
        
        # Create a README file
        self._create_readme(dataset_dir, data_filename, media_files, metadata)
        
        # Export in the requested format
        if output_format.lower() == 'zip':
            return self._create_zip(dataset_dir)
        else:
            return dataset_dir
    
    def _create_readme(self, 
                       dataset_dir: str, 
                       data_filename: str, 
                       media_files: Optional[List[str]], 
                       metadata: Optional[Dict[str, Any]]) -> None:
        """
        Create a README file for the dataset
        
        Args:
            dataset_dir: Path to the dataset directory
            data_filename: Name of the data file
            media_files: Optional list of media file paths
            metadata: Optional metadata about the dataset
        """
        readme_path = os.path.join(dataset_dir, 'README.md')
        
        with open(readme_path, 'w', encoding='utf-8') as f:
            # Write header
            f.write(f"# Dataset: {os.path.basename(dataset_dir)}\n\n")
            f.write(f"Created on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            # Write metadata section if available
            if metadata:
                f.write("## Metadata\n\n")
                
                if 'topic' in metadata:
                    f.write(f"**Topic:** {metadata['topic']}\n\n")
                
                if 'description' in metadata:
                    f.write(f"**Description:** {metadata['description']}\n\n")
                
                if 'sources' in metadata:
                    f.write("**Sources:**\n\n")
                    for source in metadata['sources']:
                        f.write(f"- {source}\n")
                    f.write("\n")
            
            # Write data file section
            f.write("## Data File\n\n")
            f.write(f"The main data file is `{data_filename}`.\n\n")
            
            # Try to read and display the data structure
            try:
                file_ext = os.path.splitext(data_filename)[1].lower()
                if file_ext == '.csv':
                    df = pd.read_csv(os.path.join(dataset_dir, data_filename))
                elif file_ext in ['.xlsx', '.xls']:
                    df = pd.read_excel(os.path.join(dataset_dir, data_filename))
                elif file_ext == '.json':
                    df = pd.read_json(os.path.join(dataset_dir, data_filename))
                else:
                    df = None
                
                if df is not None:
                    f.write("### Data Structure\n\n")
                    f.write(f"Number of records: {len(df)}\n\n")
                    f.write("Columns:\n\n")
                    
                    for col in df.columns:
                        f.write(f"- `{col}`: {df[col].dtype}\n")
                    
                    f.write("\n")
            except Exception as e:
                logger.error(f"Error reading data file for README: {e}")
            
            # Write media files section if available
            if media_files:
                f.write("## Media Files\n\n")
                f.write(f"Number of media files: {len(media_files)}\n\n")
                
                # Count files by type
                file_types = {}
                for media_file in media_files:
                    ext = os.path.splitext(media_file)[1].lower()
                    file_types[ext] = file_types.get(ext, 0) + 1
                
                f.write("File types:\n\n")
                for ext, count in file_types.items():
                    f.write(f"- {ext}: {count} files\n")
                
                f.write("\n")
                f.write("Media files are stored in the `media` directory.\n\n")
            
            # Write usage section
            f.write("## Usage\n\n")
            f.write("This dataset can be loaded and analyzed using Python with pandas:\n\n")
            
            file_ext = os.path.splitext(data_filename)[1].lower()
            if file_ext == '.csv':
                f.write("```python\n")
                f.write("import pandas as pd\n\n")
                f.write(f"# Load the dataset\n")
                f.write(f"df = pd.read_csv('{data_filename}')\n\n")
                f.write("# Display basic information\n")
                f.write("print(df.info())\n")
                f.write("print(df.describe())\n")
                f.write("```\n\n")
            elif file_ext in ['.xlsx', '.xls']:
                f.write("```python\n")
                f.write("import pandas as pd\n\n")
                f.write(f"# Load the dataset\n")
                f.write(f"df = pd.read_excel('{data_filename}')\n\n")
                f.write("# Display basic information\n")
                f.write("print(df.info())\n")
                f.write("print(df.describe())\n")
                f.write("```\n\n")
            elif file_ext == '.json':
                f.write("```python\n")
                f.write("import pandas as pd\n\n")
                f.write(f"# Load the dataset\n")
                f.write(f"df = pd.read_json('{data_filename}')\n\n")
                f.write("# Display basic information\n")
                f.write("print(df.info())\n")
                f.write("print(df.describe())\n")
                f.write("```\n\n")
    
    def _create_zip(self, dataset_dir: str) -> str:
        """
        Create a ZIP archive of the dataset directory
        
        Args:
            dataset_dir: Path to the dataset directory
            
        Returns:
            Path to the ZIP file
        """
        zip_path = f"{dataset_dir}.zip"
        
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for root, _, files in os.walk(dataset_dir):
                for file in files:
                    file_path = os.path.join(root, file)
                    arc_name = os.path.relpath(file_path, os.path.dirname(dataset_dir))
                    zipf.write(file_path, arc_name)
        
        logger.info(f"Dataset exported to {zip_path}")
        return zip_path
