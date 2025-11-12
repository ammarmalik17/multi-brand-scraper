import random
import time
from typing import Dict, Any, List, Optional, Tuple
import httpx
import logging

# List of common user agents to rotate through
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.2 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:90.0) Gecko/20100101 Firefox/90.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36 Edg/91.0.864.59",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (iPad; CPU OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36 OPR/78.0.4093.184"
]

# List of common referrers to rotate through
REFERRERS = [
    "https://www.google.com/",
    "https://www.bing.com/",
    "https://www.facebook.com/",
    "https://www.instagram.com/",
    "https://www.pinterest.com/",
    "https://twitter.com/",
    "https://pk.sapphireonline.pk/",
    "https://pk.sapphireonline.pk/sale/",
    "https://pk.sapphireonline.pk/collections/",
    "https://pk.sapphireonline.pk/new-arrivals/"
]

class RequestManager:
    """
    Utility class to manage HTTP requests with anti-blocking measures.
    Implements exponential backoff, user agent rotation, and referrer spoofing.
    """
    
    def __init__(self, base_url: str, retry_count: int = 3, retry_delay: int = 5):
        """
        Initialize the request manager.
        
        Args:
            base_url: Base URL for the website
            retry_count: Maximum number of retries for failed requests
            retry_delay: Initial delay between retries (in seconds)
        """
        self.base_url = base_url
        self.retry_count = retry_count
        self.retry_delay = retry_delay
        self.logger = logging.getLogger('request_manager')
        
        # Create a client for persistent cookies
        self.session = httpx.Client()
        
        # Initialize with random user agent and referrer
        self.rotate_identity()
    
    def rotate_identity(self):
        """Rotate user agent and referrer to avoid detection."""
        user_agent = random.choice(USER_AGENTS)
        referrer = random.choice(REFERRERS)
        
        # Update session headers
        self.session.headers.update({
            'User-Agent': user_agent,
            'Referer': referrer
        })
        
        self.logger.debug(f"Rotated identity - UA: {user_agent[:20]}... | Referrer: {referrer}")
    
    def make_request(self, url: str, params: Optional[Dict[str, Any]] = None, 
                     headers: Optional[Dict[str, Any]] = None, 
                     timeout: int = 30) -> Tuple[httpx.Response, bool]:
        """
        Make a GET request with exponential backoff retry logic.
        
        Args:
            url: URL to make the request to
            params: Optional query parameters
            headers: Additional headers to include
            timeout: Request timeout in seconds
            
        Returns:
            Tuple of (Response object, success boolean)
        """
        merged_headers = dict(self.session.headers)
        if headers:
            merged_headers.update(headers)
        
        # Try the request with exponential backoff
        success = False
        current_delay = self.retry_delay
        response = None
        
        for attempt in range(self.retry_count + 1):
            try:
                # Rotate identity before each attempt
                if attempt > 0:
                    self.rotate_identity()
                    self.logger.info(f"Retry attempt {attempt}/{self.retry_count} after {current_delay}s delay")
                
                response = self.session.get(
                    url,
                    params=params,
                    headers=merged_headers,
                    timeout=timeout
                )
                
                response.raise_for_status()
                success = True
                break
                
            except httpx.HTTPStatusError as e:
                if response.status_code == 429:  # Too Many Requests
                    self.logger.warning(f"Rate limited (429). Retry attempt {attempt + 1}/{self.retry_count}")
                    wait_time = int(response.headers.get('Retry-After', current_delay))
                    time.sleep(wait_time)
                else:
                    self.logger.error(f"HTTP error {response.status_code}: {str(e)}")
                    time.sleep(current_delay)
                    
            except httpx.ConnectError as e:
                self.logger.error(f"Connection error on attempt {attempt + 1}: {str(e)}")
                time.sleep(current_delay)
                
            except httpx.TimeoutException as e:
                self.logger.error(f"Timeout error on attempt {attempt + 1}: {str(e)}")
                time.sleep(current_delay)
                
            except httpx.RequestError as e:
                self.logger.error(f"Request error on attempt {attempt + 1}: {str(e)}")
                time.sleep(current_delay)
            
            # Exponential backoff - double the delay each time
            current_delay *= 2
        
        if not success and response is None:
            # Create a fake response object for consistent return type
            response = httpx.Response(status_code=0, request=httpx.Request("GET", url))
        
        return response, success 