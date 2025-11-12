import httpx
from abc import ABC, abstractmethod
import logging
from typing import Dict, Any, Optional

class BaseScraper(ABC):
    """Base scraper class with common functionality for all brand scrapers."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the base scraper with configuration.
        
        Args:
            config: Dictionary containing scraper configuration
        """
        self.config = config
        self.base_url = config.get('base_url', '')
        self.headers = config.get('headers', {})
        self.timeout = config.get('timeout', 30)
        self.brand_name = config.get('name', '')
        self.brand_id = config.get('brand_id', '')
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(f"scraper.{self.__class__.__name__}")
    
    def make_request(self, url: str, params: Optional[Dict[str, Any]] = None) -> httpx.Response:
        """
        Make a GET request to the specified URL.
        
        Args:
            url: URL to make the request to
            params: Optional query parameters
            
        Returns:
            Response object from httpx
        """
        try:
            self.logger.info(f"Making request to {url}")
            with httpx.Client() as client:
                response = client.get(
                    url,
                    headers=self.headers,
                    params=params,
                    timeout=self.timeout
                )
            response.raise_for_status()
            return response
        except httpx.RequestError as e:
            self.logger.error(f"Request failed: {str(e)}")
            raise
    
    @abstractmethod
    def scrape(self) -> Any:
        """
        Main scraping method to be implemented by each brand scraper.
        
        Returns:
            Scraped data in the format specified by the implementation
        """
        pass 