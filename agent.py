# core/agent.py

import os
import time
import json
import logging
from typing import Dict, List, Any, Optional, Union, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

from core.llm import LLMProcessor, ScrapingTask
from core.scraper import ScraperOrchestrator
from core.processor import DataProcessor
from core.exporter import DatasetExporter
from utils.helpers import generate_unique_id, estimate_task_complexity

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ScrapingAgent:
    """Main agent class for orchestrating the web scraping process"""
    
    def __init__(self, output_dir: str):
        """
        Initialize the scraping agent
        
        Args:
            output_dir: Directory to save output files
        """
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        
        # Initialize components
        self.llm_processor = LLMProcessor()
        self.scraper_orchestrator = ScraperOrchestrator(output_dir)
        self.data_processor = DataProcessor(output_dir)
        self.dataset_exporter = DatasetExporter(output_dir)
        
        # Initialize state
        self.current_task_id = None
        self.current_task = None
        self.current_status = None
        self.progress = 0
    
    def process_request(self, user_request: str) -> Dict[str, Any]:
        """
        Process a user request and return task information
        
        Args:
            user_request: Natural language request from the user
            
        Returns:
            Dictionary with task information
        """
        # Generate a task ID
        self.current_task_id = generate_unique_id('task')
        
        # Update status
        self.current_status = "Analyzing request"
        self.progress = 10
        
        try:
            # Analyze the request
            task = self.llm_processor.analyze_request(user_request)
            self.current_task = task
            
            # Estimate complexity
            complexity, estimated_time = estimate_task_complexity(task.dict())
            
            # Update status
            self.current_status = "Request analyzed"
            self.progress = 20
            
            # Create task information
            task_info = {
                'task_id': self.current_task_id,
                'request': user_request,
                'task': task.dict(),
                'complexity': complexity,
                'estimated_time': estimated_time,
                'status': self.current_status,
                'progress': self.progress
            }
            
            return task_info
        except Exception as e:
            logger.error(f"Error processing request: {e}")
            # Return a basic error task_info
            return {
                'task_id': self.current_task_id,
                'request': user_request,
                'task': {
                    'topic': user_request,
                    'data_type': 'text',
                    'sources': [],
                    'attributes': [],
                    'filters': {},
                    'output_format': 'csv',
                    'search_queries': [user_request]
                },
                'complexity': 'Low',
                'estimated_time': 30,
                'status': 'Error analyzing request',
                'progress': 10,
                'error': str(e)
            }
    
    def execute_task(self, task_info: Dict[str, Any], callback=None) -> Dict[str, Any]:
        """
        Execute a scraping task
        
        Args:
            task_info: Task information from process_request
            callback: Optional callback function to update progress
            
        Returns:
            Dictionary with results
        """
        task_id = task_info['task_id']
        task = ScrapingTask(**task_info['task'])
        self.current_task = task
        
        # Update status
        self.current_status = "Generating scraping strategy"
        self.progress = 30
        if callback:
            callback(self.current_status, self.progress)
        
        try:
            # Generate scraping strategy
            strategy = self.llm_processor.generate_scraping_strategy(task)
            
            # Update status
            self.current_status = "Strategy generated"
            self.progress = 40
            if callback:
                callback(self.current_status, self.progress)
            
            # Determine the appropriate scraper
            if task.data_type.lower() in ['text', 'mixed']:
                scraper_type = 'requests'
                # Use Playwright if the sources likely require JavaScript
                if any(domain in source for source in task.sources 
                      for domain in ['twitter.com', 'facebook.com', 'instagram.com', 
                                     'youtube.com', 'linkedin.com', 'tiktok.com']):
                    scraper_type = 'playwright'
            elif task.data_type.lower() in ['image', 'video', 'audio']:
                scraper_type = 'media'
            else:
                scraper_type = 'requests'
            
            # Update status
            self.current_status = "Starting data collection"
            self.progress = 50
            if callback:
                callback(self.current_status, self.progress)
            
            # Collect data
            results = []
            media_files = []
            
            if scraper_type == 'media':
                # Handle media scraping
                self.current_status = "Collecting media files"
                if callback:
                    callback(self.current_status, self.progress)
                
                media_files = self.scraper_orchestrator.download_media(task.sources, task.data_type.lower())
                
                self.current_status = f"Collected {len(media_files)} media files"
                self.progress = 70
                if callback:
                    callback(self.current_status, self.progress)
                
                # Process media files metadata
                media_metadata = self.data_processor.process_media_files(media_files, task.dict())
                results = media_metadata.to_dict('records')
            else:
                # Handle regular data scraping
                priority_sources = strategy.get('priority_sources', task.sources)
                if not priority_sources and task.search_queries:
                    # If no sources but we have search queries, update status
                    self.current_status = "No direct sources provided, using search queries"
                    if callback:
                        callback(self.current_status, self.progress)
                    # We'll use the sources from the strategy, which might be empty
                
                selectors = strategy.get('selectors', {})
                
                # If no selectors are provided, create default ones
                if not selectors:
                    selectors = {attr: f"*:contains('{attr}')" for attr in task.attributes}
                
                # Use ThreadPoolExecutor for parallel scraping
                max_workers = min(5, len(priority_sources)) if priority_sources else 1
                
                if not priority_sources:
                    self.current_status = "No sources found. Try providing specific websites in your request."
                    self.progress = 100
                    if callback:
                        callback(self.current_status, self.progress)
                    
                    return {
                        'task_id': task_id,
                        'status': 'failed',
                        'error': 'No sources found for the given request'
                    }
                
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    futures = {executor.submit(self.scraper_orchestrator.scrape_url, url, selectors, scraper_type): url 
                              for url in priority_sources}
                    
                    completed = 0
                    for future in as_completed(futures):
                        url = futures[future]
                        try:
                            result = future.result()
                            if result:
                                results.append(result)
                        except Exception as e:
                            logger.error(f"Error scraping {url}: {e}")
                        
                        completed += 1
                        self.progress = 50 + int(20 * (completed / len(priority_sources)))
                        self.current_status = f"Scraped {completed}/{len(priority_sources)} sources"
                        if callback:
                            callback(self.current_status, self.progress)
            
            # Update status
            self.current_status = "Processing data"
            self.progress = 80
            if callback:
                callback(self.current_status, self.progress)
            
            # Process data
            if results:
                df = self.data_processor.process_data(results, task.dict())
                
                # Save data to file
                output_format = task.output_format.lower()
                data_file = self.data_processor.save_data(df, output_format, f"{task_id}_data")
                
                # Update status
                self.current_status = "Exporting dataset"
                self.progress = 90
                if callback:
                    callback(self.current_status, self.progress)
                
                # Export dataset
                metadata = {
                    'task_id': task_id,
                    'topic': task.topic,
                    'data_type': task.data_type,
                    'sources': task.sources,
                    'attributes': task.attributes,
                    'completion_time': time.strftime("%Y-%m-%d %H:%M:%S"),
                    'record_count': len(df)
                }
                
                dataset_path = self.dataset_exporter.export_dataset(data_file, media_files, metadata)
                
                # Update status
                self.current_status = "Dataset ready"
                self.progress = 100
                if callback:
                    callback(self.current_status, self.progress)
                
                # Return results
                return {
                    'task_id': task_id,
                    'status': 'completed',
                    'dataset_path': dataset_path,
                    'data_file': data_file,
                    'media_files': media_files if media_files else None,
                    'record_count': len(df),
                    'columns': df.columns.tolist()
                }
            else:
                # No results found
                self.current_status = "No data found"
                self.progress = 100
                if callback:
                    callback(self.current_status, self.progress)
                
                return {
                    'task_id': task_id,
                    'status': 'failed',
                    'error': 'No data found for the given request. Try being more specific with your data requirements or sources.'
                }
                
        except Exception as e:
            # Handle any exceptions during execution
            logger.error(f"Error executing task: {e}")
            self.current_status = f"Error: {str(e)}"
            self.progress = 100
            if callback:
                callback(self.current_status, self.progress)
            
            return {
                'task_id': task_id,
                'status': 'failed',
                'error': str(e)
            }
    
    def get_status(self) -> Dict[str, Any]:
        """
        Get the current status of the agent
        
        Returns:
            Dictionary with status information
        """
        return {
            'task_id': self.current_task_id,
            'status': self.current_status,
            'progress': self.progress,
            'task': self.current_task.dict() if self.current_task else None
        }
    
    def cancel_task(self) -> Dict[str, Any]:
        """
        Cancel the current task
        
        Returns:
            Dictionary with cancellation status
        """
        # Only cancel if there's an active task
        if self.current_task_id:
            old_task_id = self.current_task_id
            old_status = self.current_status
            
            # Reset state
            self.current_status = "Cancelled"
            self.progress = 0
            self.current_task_id = None
            self.current_task = None
            
            # Stop any scrapers
            self.scraper_orchestrator.close()
            
            return {
                'task_id': old_task_id,
                'status': 'cancelled',
                'previous_status': old_status
            }
        else:
            return {
                'status': 'no_task',
                'message': 'No active task to cancel'
            }
    
    def clean_up(self):
        """Clean up resources used by the agent"""
        self.scraper_orchestrator.close()