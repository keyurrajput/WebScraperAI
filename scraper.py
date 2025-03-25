# core/scraper.py

import os
import time
import random
import requests
import logging
from typing import Dict, List, Any, Optional, Union
from urllib.parse import urlparse, urljoin
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from webdriver_manager.chrome import ChromeDriverManager
from playwright.sync_api import sync_playwright

from config.config import DEFAULT_HEADERS, REQUEST_DELAY, SELENIUM_TIMEOUT, PLAYWRIGHT_TIMEOUT

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class BaseScraper:
    """Base class for web scrapers"""
    
    def __init__(self, headers: Optional[Dict[str, str]] = None):
        """
        Initialize the base scraper
        
        Args:
            headers: Custom headers for HTTP requests
        """
        self.headers = headers or DEFAULT_HEADERS
        self.session = requests.Session()
        self.session.headers.update(self.headers)
    
    def get_page(self, url: str) -> Optional[str]:
        """
        Get the HTML content of a page
        
        Args:
            url: URL to fetch
            
        Returns:
            HTML content as string or None if request failed
        """
        try:
            # Add a random delay to avoid being blocked
            time.sleep(REQUEST_DELAY * (0.5 + random.random()))
            
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            return response.text
        except Exception as e:
            logger.error(f"Error fetching {url}: {e}")
            return None
    
    def parse_html(self, html: str) -> Optional[BeautifulSoup]:
        """
        Parse HTML content with BeautifulSoup
        
        Args:
            html: HTML content as string
            
        Returns:
            BeautifulSoup object or None if parsing failed
        """
        if not html:
            return None
        
        try:
            return BeautifulSoup(html, 'html.parser')
        except Exception as e:
            logger.error(f"Error parsing HTML: {e}")
            return None
    
    def extract_links(self, soup: BeautifulSoup, base_url: str, filter_pattern: Optional[str] = None) -> List[str]:
        """
        Extract links from a BeautifulSoup object
        
        Args:
            soup: BeautifulSoup object
            base_url: Base URL for resolving relative links
            filter_pattern: Optional pattern to filter links
            
        Returns:
            List of absolute URLs
        """
        if not soup:
            return []
        
        links = []
        for a_tag in soup.find_all('a', href=True):
            href = a_tag['href']
            absolute_url = urljoin(base_url, href)
            
            # Filter URLs if pattern is provided
            if filter_pattern and filter_pattern not in absolute_url:
                continue
                
            links.append(absolute_url)
        
        return links
    
    def clean_url(self, url: str) -> str:
        """
        Clean a URL by removing query parameters and fragments
        
        Args:
            url: URL to clean
            
        Returns:
            Cleaned URL
        """
        parsed = urlparse(url)
        return f"{parsed.scheme}://{parsed.netloc}{parsed.path}"


class RequestsScraper(BaseScraper):
    """Scraper implementation using the requests library"""
    
    def scrape_data(self, url: str, selectors: Dict[str, str]) -> Dict[str, Any]:
        """
        Scrape data from a URL using specified selectors
        
        Args:
            url: URL to scrape
            selectors: Dictionary mapping attribute names to CSS selectors
            
        Returns:
            Dictionary of scraped data
        """
        html = self.get_page(url)
        if not html:
            return {}
        
        soup = self.parse_html(html)
        if not soup:
            return {}
        
        result = {'url': url}
        
        for attr_name, selector in selectors.items():
            try:
                elements = soup.select(selector)
                if elements:
                    if len(elements) == 1:
                        result[attr_name] = elements[0].get_text(strip=True)
                    else:
                        result[attr_name] = [el.get_text(strip=True) for el in elements]
                else:
                    result[attr_name] = None
            except Exception as e:
                logger.error(f"Error extracting {attr_name} with selector {selector}: {e}")
                result[attr_name] = None
        
        return result


class SeleniumScraper(BaseScraper):
    """Scraper implementation using Selenium for JavaScript-rendered pages"""
    
    def __init__(self, headless: bool = True):
        """
        Initialize the Selenium scraper
        
        Args:
            headless: Whether to run Chrome in headless mode
        """
        super().__init__()
        self.headless = headless
        self.driver = None
    
    def _initialize_driver(self):
        """Initialize the Chrome WebDriver"""
        if self.driver:
            return
        
        chrome_options = Options()
        if self.headless:
            chrome_options.add_argument("--headless")
        
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        
        # Add headers
        for key, value in self.headers.items():
            chrome_options.add_argument(f'--header={key}:{value}')
        
        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=chrome_options)
    
    def get_page(self, url: str) -> Optional[str]:
        """
        Get the HTML content of a page using Selenium
        
        Args:
            url: URL to fetch
            
        Returns:
            HTML content as string or None if request failed
        """
        try:
            self._initialize_driver()
            
            # Add a random delay to avoid being blocked
            time.sleep(REQUEST_DELAY * (0.5 + random.random()))
            
            self.driver.get(url)
            
            # Wait for the page to load
            WebDriverWait(self.driver, SELENIUM_TIMEOUT).until(
                EC.presence_of_element_located(("tag name", "body"))
            )
            
            return self.driver.page_source
        except Exception as e:
            logger.error(f"Error fetching {url} with Selenium: {e}")
            return None
    
    def scrape_data(self, url: str, selectors: Dict[str, str]) -> Dict[str, Any]:
        """
        Scrape data from a URL using specified selectors with Selenium
        
        Args:
            url: URL to scrape
            selectors: Dictionary mapping attribute names to CSS selectors
            
        Returns:
            Dictionary of scraped data
        """
        html = self.get_page(url)
        if not html:
            return {}
        
        soup = self.parse_html(html)
        if not soup:
            return {}
        
        result = {'url': url}
        
        for attr_name, selector in selectors.items():
            try:
                elements = soup.select(selector)
                if elements:
                    if len(elements) == 1:
                        result[attr_name] = elements[0].get_text(strip=True)
                    else:
                        result[attr_name] = [el.get_text(strip=True) for el in elements]
                else:
                    result[attr_name] = None
            except Exception as e:
                logger.error(f"Error extracting {attr_name} with selector {selector}: {e}")
                result[attr_name] = None
        
        return result
        
    def close(self):
        """Close the WebDriver"""
        if self.driver:
            self.driver.quit()
            self.driver = None


class PlaywrightScraper(BaseScraper):
    """Scraper implementation using Playwright for complex JavaScript-rendered pages"""
    
    def get_page(self, url: str) -> Optional[str]:
        """
        Get the HTML content of a page using Playwright
        
        Args:
            url: URL to fetch
            
        Returns:
            HTML content as string or None if request failed
        """
        try:
            # Add a random delay to avoid being blocked
            time.sleep(REQUEST_DELAY * (0.5 + random.random()))
            
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                context = browser.new_context(
                    user_agent=self.headers.get("User-Agent"),
                    viewport={"width": 1920, "height": 1080}
                )
                
                page = context.new_page()
                page.set_default_timeout(PLAYWRIGHT_TIMEOUT)
                
                # Set extra headers
                page.set_extra_http_headers(self.headers)
                
                page.goto(url, wait_until="networkidle")
                html = page.content()
                
                browser.close()
                return html
        except Exception as e:
            logger.error(f"Error fetching {url} with Playwright: {e}")
            return None
    
    def scrape_data(self, url: str, selectors: Dict[str, str]) -> Dict[str, Any]:
        """
        Scrape data from a URL using specified selectors with Playwright
        
        Args:
            url: URL to scrape
            selectors: Dictionary mapping attribute names to CSS selectors
            
        Returns:
            Dictionary of scraped data
        """
        html = self.get_page(url)
        if not html:
            return {}
        
        soup = self.parse_html(html)
        if not soup:
            return {}
        
        result = {'url': url}
        
        for attr_name, selector in selectors.items():
            try:
                elements = soup.select(selector)
                if elements:
                    if len(elements) == 1:
                        result[attr_name] = elements[0].get_text(strip=True)
                    else:
                        result[attr_name] = [el.get_text(strip=True) for el in elements]
                else:
                    result[attr_name] = None
            except Exception as e:
                logger.error(f"Error extracting {attr_name} with selector {selector}: {e}")
                result[attr_name] = None
        
        return result


class MediaScraper(BaseScraper):
    """Scraper specialized for media content (images, videos, audio)"""
    
    def __init__(self, output_dir: str):
        """
        Initialize the media scraper
        
        Args:
            output_dir: Directory to save media files
        """
        super().__init__()
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
    
    def download_file(self, url: str, filename: Optional[str] = None) -> Optional[str]:
        """
        Download a file from a URL
        
        Args:
            url: URL of the file to download
            filename: Optional filename to save as
            
        Returns:
            Path to the downloaded file or None if download failed
        """
        try:
            # Add a random delay to avoid being blocked
            time.sleep(REQUEST_DELAY * (0.5 + random.random()))
            
            response = self.session.get(url, stream=True, timeout=30)
            response.raise_for_status()
            
            # Generate filename if not provided
            if not filename:
                content_disposition = response.headers.get('content-disposition')
                if content_disposition and 'filename=' in content_disposition:
                    filename = content_disposition.split('filename=')[1].strip('"\'')
                else:
                    url_path = urlparse(url).path
                    filename = os.path.basename(url_path)
                    
                    # If filename is empty or doesn't have an extension, create a generic one
                    if not filename or '.' not in filename:
                        content_type = response.headers.get('content-type', '')
                        if 'image' in content_type:
                            ext = content_type.split('/')[-1] if '/' in content_type else 'jpg'
                            filename = f"image_{int(time.time())}_{random.randint(1000, 9999)}.{ext}"
                        elif 'video' in content_type:
                            ext = content_type.split('/')[-1] if '/' in content_type else 'mp4'
                            filename = f"video_{int(time.time())}_{random.randint(1000, 9999)}.{ext}"
                        elif 'audio' in content_type:
                            ext = content_type.split('/')[-1] if '/' in content_type else 'mp3'
                            filename = f"audio_{int(time.time())}_{random.randint(1000, 9999)}.{ext}"
                        else:
                            filename = f"file_{int(time.time())}_{random.randint(1000, 9999)}"
            
            file_path = os.path.join(self.output_dir, filename)
            
            # Save the file
            with open(file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            
            return file_path
        except Exception as e:
            logger.error(f"Error downloading {url}: {e}")
            return None
    
    def extract_media_urls(self, soup: BeautifulSoup, base_url: str, media_type: str = 'image') -> List[str]:
        """
        Extract media URLs from a BeautifulSoup object
        
        Args:
            soup: BeautifulSoup object
            base_url: Base URL for resolving relative URLs
            media_type: Type of media to extract ('image', 'video', 'audio')
            
        Returns:
            List of media URLs
        """
        if not soup:
            return []
        
        urls = []
        
        if media_type == 'image':
            # Find image tags
            for img in soup.find_all('img', src=True):
                src = img['src']
                if src.startswith('data:'):
                    # Skip data URLs
                    continue
                absolute_url = urljoin(base_url, src)
                urls.append(absolute_url)
        
        elif media_type == 'video':
            # Find video tags
            for video in soup.find_all('video'):
                # Check source tags within video
                for source in video.find_all('source', src=True):
                    src = source['src']
                    absolute_url = urljoin(base_url, src)
                    urls.append(absolute_url)
                
                # Check video src attribute
                if video.get('src'):
                    src = video['src']
                    absolute_url = urljoin(base_url, src)
                    urls.append(absolute_url)
            
            # Check for iframe embeds (YouTube, Vimeo, etc.)
            for iframe in soup.find_all('iframe', src=True):
                src = iframe['src']
                if any(domain in src for domain in ['youtube.com', 'youtu.be', 'vimeo.com']):
                    urls.append(src)
        
        elif media_type == 'audio':
            # Find audio tags
            for audio in soup.find_all('audio'):
                # Check source tags within audio
                for source in audio.find_all('source', src=True):
                    src = source['src']
                    absolute_url = urljoin(base_url, src)
                    urls.append(absolute_url)
                
                # Check audio src attribute
                if audio.get('src'):
                    src = audio['src']
                    absolute_url = urljoin(base_url, src)
                    urls.append(absolute_url)
        
        return urls

    def download_media_from_page(self, url: str, media_type: str = 'image') -> List[str]:
        """
        Download all media of a specific type from a page
        
        Args:
            url: URL of the page
            media_type: Type of media to download ('image', 'video', 'audio')
            
        Returns:
            List of paths to downloaded files
        """
        html = self.get_page(url)
        if not html:
            return []
        
        soup = self.parse_html(html)
        if not soup:
            return []
        
        media_urls = self.extract_media_urls(soup, url, media_type)
        
        downloaded_files = []
        for media_url in media_urls:
            file_path = self.download_file(media_url)
            if file_path:
                downloaded_files.append(file_path)
        
        return downloaded_files


class ScraperFactory:
    """Factory class for creating appropriate scrapers"""
    
    @staticmethod
    def create_scraper(scraper_type: str, **kwargs) -> BaseScraper:
        """
        Create a scraper of the specified type
        
        Args:
            scraper_type: Type of scraper to create
            **kwargs: Additional arguments for the scraper
            
        Returns:
            Scraper instance
        """
        if scraper_type == 'requests':
            return RequestsScraper(**kwargs)
        elif scraper_type == 'selenium':
            return SeleniumScraper(**kwargs)
        elif scraper_type == 'playwright':
            return PlaywrightScraper(**kwargs)
        elif scraper_type == 'media':
            return MediaScraper(**kwargs)
        else:
            raise ValueError(f"Unsupported scraper type: {scraper_type}")


class ScraperOrchestrator:
    """Class for orchestrating multiple scrapers"""
    
    def __init__(self, output_dir: str):
        """
        Initialize the scraper orchestrator
        
        Args:
            output_dir: Directory to save output files
        """
        self.output_dir = output_dir
        self.scrapers = {}
    
    def get_scraper(self, scraper_type: str) -> BaseScraper:
        """
        Get or create a scraper of the specified type
        
        Args:
            scraper_type: Type of scraper to get
            
        Returns:
            Scraper instance
        """
        if scraper_type not in self.scrapers:
            kwargs = {}
            if scraper_type == 'media':
                kwargs['output_dir'] = os.path.join(self.output_dir, 'media')
            
            self.scrapers[scraper_type] = ScraperFactory.create_scraper(scraper_type, **kwargs)
        
        return self.scrapers[scraper_type]
    
    def scrape_url(self, url: str, selectors: Dict[str, str], scraper_type: str = 'requests') -> Dict[str, Any]:
        """
        Scrape data from a URL
        
        Args:
            url: URL to scrape
            selectors: Dictionary mapping attribute names to CSS selectors
            scraper_type: Type of scraper to use
            
        Returns:
            Dictionary of scraped data
        """
        scraper = self.get_scraper(scraper_type)
        return scraper.scrape_data(url, selectors)
    
    def scrape_urls(self, urls: List[str], selectors: Dict[str, str], scraper_type: str = 'requests') -> List[Dict[str, Any]]:
        """
        Scrape data from multiple URLs
        
        Args:
            urls: List of URLs to scrape
            selectors: Dictionary mapping attribute names to CSS selectors
            scraper_type: Type of scraper to use
            
        Returns:
            List of dictionaries with scraped data
        """
        results = []
        for url in urls:
            result = self.scrape_url(url, selectors, scraper_type)
            if result:
                results.append(result)
        
        return results
    
    def download_media(self, urls: List[str], media_type: str = 'image') -> List[str]:
        """
        Download media from multiple URLs
        
        Args:
            urls: List of URLs to download from
            media_type: Type of media to download
            
        Returns:
            List of paths to downloaded files
        """
        media_scraper = self.get_scraper('media')
        
        downloaded_files = []
        for url in urls:
            if media_type in ['image', 'video', 'audio']:
                # For web pages containing media
                files = media_scraper.download_media_from_page(url, media_type)
                downloaded_files.extend(files)
            else:
                # For direct media URLs
                file_path = media_scraper.download_file(url)
                if file_path:
                    downloaded_files.append(file_path)
        
        return downloaded_files
    
    def close(self):
        """Close all scrapers"""
        for scraper_type, scraper in self.scrapers.items():
            if hasattr(scraper, 'close'):
                scraper.close()
