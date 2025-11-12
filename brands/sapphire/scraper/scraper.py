from bs4 import BeautifulSoup
from typing import Dict, Any, List
import time
import random
import concurrent.futures

from brands.base_scraper import BaseScraper
from src.utils.request_utils import RequestManager

class SapphireScraper(BaseScraper):
    """Scraper for Sapphire Online website."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize Sapphire scraper with configuration.
        
        Args:
            config: Dictionary containing scraper configuration
        """
        super().__init__(config)
        self.search_api_url = config.get('search_api_url', '')
        self.categories = config.get('categories', {})
        self.page_size = config.get('page_size', 12)
        self.sleep_between_requests = config.get('sleep_between_requests', 2)
        self.retry_count = config.get('retry_count', 3)
        self.retry_delay = config.get('retry_delay', 5)
        self.product_limit = config.get('product_limit', 0)  # 0 means no limit
        self.max_workers = config.get('max_workers', 3)  # Maximum number of parallel workers
        
        # Create a request manager with anti-blocking features
        self.request_manager = RequestManager(
            base_url=self.base_url,
            retry_count=self.retry_count,
            retry_delay=self.retry_delay
        )
    
    def scrape(self) -> List[Dict[str, Any]]:
        """
        Scrape product data from Sapphire Online website using parallel scraping for categories.
        
        Returns:
            List of dictionaries containing product data
        """
        all_products = []
        
        # If there's only one category, use the regular sequential approach
        if len(self.categories) <= 1:
            return self._scrape_sequential()
            
        # Use ThreadPoolExecutor for parallel scraping
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(self.max_workers, len(self.categories))) as executor:
            # Create a partial function for each category
            futures = []
            for category_name, category_id in self.categories.items():
                # Create a dedicated request manager for each thread to avoid conflicts
                request_manager = RequestManager(
                    base_url=self.base_url,
                    retry_count=self.retry_count,
                    retry_delay=self.retry_delay
                )
                
                future = executor.submit(
                    self._scrape_category, 
                    category_name, 
                    category_id, 
                    request_manager,
                    self.product_limit // len(self.categories) if self.product_limit > 0 else 0
                )
                futures.append(future)
            
            # Process results as they complete
            for future in concurrent.futures.as_completed(futures):
                try:
                    category_products = future.result()
                    all_products.extend(category_products)
                    self.logger.info(f"Received {len(category_products)} products from a category thread")
                except Exception as e:
                    self.logger.error(f"Error in category scraping thread: {str(e)}")
        
        # If we have a product limit, respect it
        if self.product_limit > 0 and len(all_products) > self.product_limit:
            all_products = all_products[:self.product_limit]
            
        return all_products
    
    def _scrape_sequential(self) -> List[Dict[str, Any]]:
        """
        Original sequential scraping method for backwards compatibility.
        
        Returns:
            List of dictionaries containing product data
        """
        all_products = []
        
        for category_name, category_id in self.categories.items():
            category_products = self._scrape_category(category_name, category_id, self.request_manager, self.product_limit)
            all_products.extend(category_products)
            
            # Check if we've reached the product limit
            if self.product_limit > 0 and len(all_products) >= self.product_limit:
                return all_products[:self.product_limit]
                
        return all_products
    
    def _scrape_category(self, category_name: str, category_id: str, 
                         request_manager: RequestManager, product_limit: int = 0) -> List[Dict[str, Any]]:
        """
        Scrape a single category.
        
        Args:
            category_name: Name of the category
            category_id: ID of the category
            request_manager: RequestManager instance for making requests
            product_limit: Maximum number of products to scrape (0 means no limit)
            
        Returns:
            List of dictionaries containing product data
        """
        category_products = []
        self.logger.info(f"Scraping category: {category_name}")
        
        start = 0
        page = 1
        has_more_products = True
        
        while has_more_products:
            self.logger.info(f"Scraping category {category_name} page {page} (start index: {start})")
            
            # Check if we've reached the product limit
            if product_limit > 0 and len(category_products) >= product_limit:
                self.logger.info(f"Reached product limit of {product_limit} for category {category_name}. Stopping scraping.")
                return category_products[:product_limit]
            
            # Prepare URL parameters
            params = {
                'cgid': category_id,
                'start': start,
                'sz': self.page_size
            }
            
            # Additional headers specific to this AJAX request
            headers = {
                'X-Requested-With': 'XMLHttpRequest',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Referer': f"{self.base_url}/{category_id}/",
            }
            
            # Make request with the request manager
            try:
                response, success = request_manager.make_request(
                    url=self.search_api_url,
                    params=params,
                    headers=headers,
                    timeout=self.timeout
                )
                
                if not success:
                    self.logger.error(f"Failed to retrieve {category_name} page {page} after multiple attempts. Status code: {response.status_code}")
                    has_more_products = False
                    break
                
                # Parse the HTML
                soup = BeautifulSoup(response.content, 'lxml')
                
                # Check if there's a message indicating no more products
                no_results = soup.select_one('.no-results')
                if no_results:
                    self.logger.info(f"Found 'no results' message for {category_name}. Stopping pagination.")
                    has_more_products = False
                    break
                
                # Extract product information
                products = self._extract_products(soup, category_name)
                
                if not products:
                    self.logger.info(f"No products found on {category_name} page {page}. Stopping pagination.")
                    has_more_products = False
                    break
                
                # Check if adding these products would exceed the limit
                if product_limit > 0:
                    remaining = product_limit - len(category_products)
                    if remaining < len(products):
                        self.logger.info(f"Taking only {remaining} of {len(products)} products to meet limit of {product_limit}")
                        category_products.extend(products[:remaining])
                        return category_products
                    
                category_products.extend(products)
                self.logger.info(f"Found {len(products)} products on {category_name} page {page}. Total for category: {len(category_products)}")
                
                # If we got fewer products than the page size, we've reached the end
                if len(products) < self.page_size:
                    self.logger.info(f"Reached end of products for {category_name} (got {len(products)} < page size {self.page_size})")
                    has_more_products = False
                    break
                
                # Sleep to avoid rate limiting - varying times to seem more human-like
                sleep_time = self.sleep_between_requests + random.uniform(0.5, 2.0)
                self.logger.debug(f"Sleeping for {sleep_time:.2f} seconds between requests")
                time.sleep(sleep_time)
                
                # Next page
                start += self.page_size
                page += 1
                
            except Exception as e:
                self.logger.error(f"Unexpected error in {category_name} scraping: {str(e)}")
                has_more_products = False
                break
                
        return category_products
    
    def _extract_products(self, soup: BeautifulSoup, category: str) -> List[Dict[str, Any]]:
        """
        Extract product information from the parsed HTML.
        
        Args:
            soup: BeautifulSoup object of parsed HTML
            category: Category name
            
        Returns:
            List of dictionaries containing product data
        """
        products = []
        
        # Changed selector to match the product tiles
        product_elements = soup.select('.product-tile')
        
        for element in product_elements:
            try:
                # Get product ID from data-pid attribute
                sku = element.get('data-pid', '')
                
                # Get product name from the link inside pdp-link
                name_element = element.select_one('.pdp-link a.link')
                if not name_element:
                    continue
                    
                name = name_element.text.strip()
                
                # Parse product name logically:
                # - Product type is typically the last word (or last two words for special cases like "3 Piece")
                # - Fabric type is typically the word before the product type
                words = name.split()
                
                # Initialize defaults
                product_type = ""
                fabric_type = ""
                fabric_style = ""
                
                # Handle special case with dash format: "3 Piece - Embroidered Karandi Suit"
                if "-" in name:
                    parts = [p.strip() for p in name.split("-")]
                    
                    # First part before dash often contains product type like "3 Piece"
                    if parts[0] in ["3 Piece", "2 Piece"]:
                        product_type = parts[0]
                    
                    # Second part after dash often contains fabric style and type
                    if len(parts) > 1:
                        second_part_words = parts[1].split()
                        
                        # If first word in second part is a style
                        if second_part_words and second_part_words[0] in ["Printed", "Embroidered", "Dyed", "Digital", "Embellished"]:
                            fabric_style = second_part_words[0]
                            
                            # Last word is often the product like "Suit", "Shirt"
                            if len(second_part_words) > 2:
                                # Fabric type is everything between style and product
                                fabric_type = " ".join(second_part_words[1:-1])
                                
                                # Update product type if it wasn't set earlier or add to it
                                if not product_type:
                                    product_type = second_part_words[-1]
                                else:
                                    # Keep the original product type (3 Piece)
                                    pass
                            elif len(second_part_words) == 2:
                                # If only two words, assume second is fabric type
                                fabric_type = second_part_words[1]
                else:
                    # Extract fabric style from name (usually first word)
                    if words and words[0] in ["Printed", "Embroidered", "Dyed", "Digital", "Embellished"]:
                        fabric_style = words[0]
                    
                    # Handle special cases first (multi-word product types)
                    if len(words) >= 2 and (words[-2] + " " + words[-1] in ["3 Piece", "2 Piece"]):
                        product_type = words[-2] + " " + words[-1]
                        
                        # Extract fabric type from remaining words before product type
                        if len(words) >= 4:
                            start_idx = 1 if fabric_style else 0
                            end_idx = -2
                            fabric_type = " ".join(words[start_idx:end_idx])
                    else:
                        # Standard case: last word is product type (like "Shirt", "Kurta")
                        if words:
                            product_type = words[-1]
                            
                            # Extract fabric type from remaining words before product type
                            if len(words) >= 3:
                                start_idx = 1 if fabric_style else 0
                                end_idx = -1
                                fabric_type = " ".join(words[start_idx:end_idx])
                
                # Special handling for compound fabric types (e.g., "Raw Silk")
                if len(words) >= 3 and words[-2] == "Silk" and words[-3] in ["Raw", "Pure"]:
                    fabric_type = words[-3] + " " + words[-2]
                elif len(words) >= 3 and words[-2] == "Cotton" and words[-3] in ["Egyptian", "Pima", "Organic"]:
                    fabric_type = words[-3] + " " + words[-2]
                
                # Get product URL from the link
                product_url = name_element.get('href', '')
                if product_url and not product_url.startswith('http'):
                    product_url = self.base_url + product_url
                
                # Get product subtitle/category early
                subtitle_element = element.select_one('.subtitle')
                subtitle = subtitle_element.text.strip() if subtitle_element else ''
                
                # Extract category from URL path (part after collections/)
                category = ""
                collection = ""
                piece_info = ""  # New variable for piece information
                
                if product_url and 'collections/' in product_url:
                    try:
                        url_parts = product_url.split('/')
                        collections_index = url_parts.index('collections')
                        if collections_index + 1 < len(url_parts):
                            # Get the part after 'collections/'
                            category_slug = url_parts[collections_index + 1]
                            
                            # Extract piece info if present
                            if "one-piece" in category_slug or "two-piece" in category_slug or "three-piece" in category_slug:
                                # Extract the piece info (e.g., "One Piece" from "one-piece-unstitched")
                                if "one-piece" in category_slug:
                                    piece_info = "One Piece"
                                    # Remove "one-piece-" from the category_slug
                                    category_slug = category_slug.replace("one-piece-", "")
                                elif "two-piece" in category_slug:
                                    piece_info = "Two Piece"
                                    category_slug = category_slug.replace("two-piece-", "")
                                elif "three-piece" in category_slug:
                                    piece_info = "Three Piece"
                                    category_slug = category_slug.replace("three-piece-", "")
                            
                            # Convert slug to readable format (replace hyphens with spaces, capitalize)
                            if category_slug:
                                category = category_slug.replace('-', ' ').title()
                                
                                # Try to extract collection information
                                if "Summer" in subtitle:
                                    collection = "Summer"
                                elif "Winter" in subtitle:
                                    collection = "Winter"
                                elif "Spring" in subtitle:
                                    collection = "Spring"
                                elif "Fall" in subtitle:
                                    collection = "Fall"
                    except Exception as e:
                        self.logger.warning(f"Failed to extract category from URL: {str(e)}")
                
                # If category is still empty, fall back to original approach
                if not category:
                    category = subtitle if subtitle else ""
                
                # If sku is empty, try to extract it from the URL
                if not sku and product_url:
                    # URLs typically have format: /products/sku.html
                    try:
                        url_parts = product_url.split('/')
                        if 'products' in url_parts:
                            products_index = url_parts.index('products')
                            if products_index + 1 < len(url_parts):
                                # Extract ID before .html
                                sku = url_parts[products_index + 1].split('.')[0]
                    except Exception as e:
                        self.logger.warning(f"Failed to extract sku from URL: {str(e)}")
                
                # Use subtitle only for collection information if not already extracted
                if not collection:
                    if "Summer" in subtitle:
                        collection = "Summer"
                    elif "Winter" in subtitle:
                        collection = "Winter"
                    elif "Spring" in subtitle:
                        collection = "Spring"
                    elif "Fall" in subtitle:
                        collection = "Fall"
                
                # Get current price from sales value
                price_element = element.select_one('.price .sales .value')
                price = price_element.text.strip() if price_element else 'N/A'
                
                # Get original price from del tag
                original_price_element = element.select_one('.price del .value')
                original_price = ''
                if original_price_element:
                    # Clean up the original price by removing newlines and extra text
                    raw_price = original_price_element.text.strip()
                    # Extract only the price part (Rs.X,XXX.XX)
                    if 'Rs.' in raw_price:
                        original_price = raw_price.split('Rs.')[1].strip()
                        original_price = f"Rs.{original_price.split()[0]}"
                    else:
                        original_price = raw_price.split()[0] if raw_price else ''
                else:
                    original_price = price
                
                # Get primary image URL - using data-src attribute instead of src
                primary_image = element.select_one('.plp-dual-image img.tile-image')
                primary_image_url = ''
                if primary_image:
                    # Try data-src first, then fall back to src if needed
                    primary_image_url = primary_image.get('data-src', '')
                    if not primary_image_url:
                        primary_image_url = primary_image.get('src', '')
                    
                    # Remove size constraints from the URL
                    if primary_image_url and '?' in primary_image_url:
                        primary_image_url = primary_image_url.split('?')[0]
                
                # Get hover image URL if available
                hover_image = element.select_one('.plp-dual-image img.hover-image')
                hover_image_url = ''
                if hover_image:
                    hover_image_url = hover_image.get('data-src', '')
                    if not hover_image_url:
                        hover_image_url = hover_image.get('src', '')
                    
                    # Remove size constraints from the URL
                    if hover_image_url and '?' in hover_image_url:
                        hover_image_url = hover_image_url.split('?')[0]
                
                # Calculate discount if original price is different
                discount = None
                if original_price != price and original_price != 'N/A' and price != 'N/A':
                    try:
                        # Clean price strings and convert to float
                        original_price_clean = float(original_price.replace('Rs.', '').replace(',', '').strip())
                        price_clean = float(price.replace('Rs.', '').replace(',', '').strip())
                        
                        if original_price_clean > 0:
                            discount = round(((original_price_clean - price_clean) / original_price_clean) * 100)
                    except (ValueError, TypeError):
                        pass
                
                # Check if the product is in stock or sold out
                is_in_stock = True  # Default to in stock
                
                # Look for sold-out badge or indicator
                sold_out_element = element.select_one('.badge-wrapper .sold-out, .span-sold-out')
                if sold_out_element:
                    is_in_stock = False
                
                # Combine images into an array
                img_url = []
                if primary_image_url:
                    img_url.append(primary_image_url)
                if hover_image_url:
                    img_url.append(hover_image_url)
                
                product = {
                    'brand': 'Sapphire',
                    'brand_id': self.brand_id,
                    'sku': sku,
                    'name': name,
                    'slug': name.lower().replace(' ', '-').replace('--', '-'),
                    'fabric_style': fabric_style,
                    'fabric_type': fabric_type,
                    'product_type': product_type,
                    'category': category,
                    'collection': collection,
                    'piece_info': piece_info,
                    'url': product_url,
                    'img_url': img_url,
                    'price': price,
                    'original_price': original_price,
                    'discount_percentage': discount,
                    'on_sale': discount is not None and discount > 0,
                    'in_stock': is_in_stock
                }
                
                products.append(product)
                
            except Exception as e:
                self.logger.warning(f"Failed to extract product data: {str(e)}")
        
        return products 