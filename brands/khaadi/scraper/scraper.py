from bs4 import BeautifulSoup
from typing import Dict, Any, List, Union
import time
import random
import json
import concurrent.futures
import datetime

from brands.base_scraper import BaseScraper
from src.utils.request_utils import RequestManager

class KhaadiScraper(BaseScraper):
    """Scraper for Khaadi website."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize Khaadi scraper with configuration.
        
        Args:
            config: Dictionary containing scraper configuration
        """
        super().__init__(config)
        self.search_api_url = config.get('search_api_url', '')
        self.categories = config.get('categories', {})
        self.page_size = config.get('page_size', 32)
        self.sleep_between_requests = config.get('sleep_between_requests', 2)
        self.retry_count = config.get('retry_count', 3)
        self.retry_delay = config.get('retry_delay', 5)
        self.product_limit = config.get('product_limit', 0)  # 0 means no limit
        self.max_workers = config.get('max_workers', 3)  # Maximum number of parallel workers
        
        # Define Khaadi-specific referrers for more realistic browsing
        khaadi_specific_referrers = [
            "https://pk.khaadi.com/",
            "https://pk.khaadi.com/new-in/",
            "https://pk.khaadi.com/sale/",
            "https://pk.khaadi.com/fabrics/",
            "https://pk.khaadi.com/ready-to-wear/",
            "https://pk.khaadi.com/collections/",
            "https://www.google.com/search?q=khaadi+pakistan",
            "https://www.facebook.com/khaadi/",
            "https://www.instagram.com/khaadi/"
        ]
        
        # Create a request manager with anti-blocking features
        self.request_manager = RequestManager(
            base_url=self.base_url,
            retry_count=self.retry_count,
            retry_delay=self.retry_delay
        )
        
        # Add Khaadi-specific referrers to the request manager
        self._update_request_manager_referrers(self.request_manager, khaadi_specific_referrers)
        
        # Initial rotation to set a random identity
        self.request_manager.rotate_identity()
    
    def _update_request_manager_referrers(self, request_manager: RequestManager, additional_referrers: List[str]):
        """
        Update the request manager with additional website-specific referrers.
        
        Args:
            request_manager: The RequestManager instance to update
            additional_referrers: List of additional referrer URLs
        """
        try:
            # Access the REFERRERS list in the RequestManager instance
            if hasattr(request_manager, 'session') and hasattr(request_manager, 'REFERRERS'):
                # Direct attribute access if available
                request_manager.REFERRERS.extend(additional_referrers)
            else:
                # If REFERRERS is a class variable, we need to modify it differently
                from src.utils.request_utils import REFERRERS
                REFERRERS.extend(additional_referrers)
                
            self.logger.info(f"Added {len(additional_referrers)} Khaadi-specific referrers to request manager")
        except (AttributeError, ImportError) as e:
            self.logger.warning(f"Could not update referrers in request manager: {str(e)}")
    
    def _convert_to_array(self, field_value):
        """
        Convert a string field with multiple words into an array of words.
        
        Args:
            field_value: The string value to convert
            
        Returns:
            List of words if input is a non-empty string, otherwise returns the input unchanged
        """
        if field_value and isinstance(field_value, str):
            # Remove pipe characters
            cleaned_value = field_value.replace('|', '')
            return [word.strip() for word in cleaned_value.split() if word.strip()]
        return field_value
    
    def scrape(self) -> List[Dict[str, Any]]:
        """
        Scrape product data from Khaadi website using parallel scraping for categories.
        
        Returns:
            List of dictionaries containing product data
        """
        all_products = []
        
        # If there's only one category, use the regular sequential approach
        if len(self.categories) <= 1:
            all_products = self._scrape_sequential()
        else:
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
            
        # Check if we need to enhance products with detailed information
        enhance_with_details = self.config.get('enhance_with_details', False)
        if enhance_with_details and all_products:
            all_products = self.enhance_products_with_details(
                all_products, 
                max_workers=self.config.get('detail_workers', 3)
            )
            
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
                'Accept': 'text/html, */*; q=0.01',
                'Referer': f"{self.base_url}/{category_id}/",
            }
            
            # Make request with the request manager
            try:
                # Rotate identity before making the request to avoid blocking
                request_manager.rotate_identity()
                
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
        
        # Get product tiles
        product_elements = soup.select(self.config.get('product_selector', '.product-tile'))
        
        for element in product_elements:
            try:
                # Get parent div with data-pid attribute (look at parent element if needed)
                parent_element = element.parent
                if parent_element and parent_element.has_attr('data-pid'):
                    sku = parent_element.get('data-pid', '')
                else:
                    # If parent doesn't have it, check the element itself
                    sku = element.get('data-pid', '')
                
                # Get product data-gtmdata for additional info
                gtm_data = {}
                if parent_element and parent_element.has_attr('data-gtmdata'):
                    try:
                        gtm_data = json.loads(parent_element.get('data-gtmdata', '{}'))
                    except json.JSONDecodeError:
                        pass
                
                # Get product name from the pdp-link-heading
                name_element = element.select_one(self.config.get('name_selector', '.pdp-link .pdp-link-heading'))
                if not name_element:
                    continue
                    
                name = name_element.text.strip()
                
                # Get product brand/fabric info
                brand_element = element.select_one(self.config.get('product_brand_selector', '.product-brand .text-truncate'))
                brand_info = brand_element.text.strip() if brand_element else ''
                
                # Parse product name and fabric info logically
                product_type = ""
                
                # First try to extract from name using position-based logic
                words = name.split()
                
                # Determine category from product name
                item_category = ""
                if "Fabrics" in name:
                    item_category = "Unstitched"
                elif "Ready" in name or "RTW" in name:
                    item_category = "Ready to Wear"
                
                # Handle special cases first (multi-word product types)
                if len(words) >= 2 and (words[-2] + " " + words[-1] in ["3 Piece", "2 Piece"]):
                    product_type = words[-2] + " " + words[-1]
                    # For "Fabrics 3 Piece", the category should be "Unstitched"
                    if len(words) >= 3 and words[-3] == "Fabrics":
                        item_category = "Unstitched"
                else:
                    # Standard case: last word is product type
                    if words:
                        product_type = words[-1]
                
                # Get product URL from the link
                pdp_link = element.select_one(self.config.get('pdp_link_selector', '.pdp-link a.link'))
                product_url = pdp_link.get('href', '') if pdp_link else ''
                if product_url and not product_url.startswith('http'):
                    product_url = self.base_url + product_url
                
                # If sku is empty, try to extract it from the URL
                if not sku and product_url:
                    # URLs typically have format: /fabrics-3-piece/MT11-VG_NAVY.html
                    try:
                        url_parts = product_url.split('/')
                        if len(url_parts) > 1:
                            # Extract ID from the last part before .html
                            filename = url_parts[-1].split('.')[0]
                            if '_' in filename:
                                sku = filename
                    except Exception as e:
                        self.logger.warning(f"Failed to extract sku from URL: {str(e)}")
                
                # Get current price from sales value
                price_element = element.select_one(self.config.get('price_selector', '.price .sales .value'))
                price = ''
                if price_element:
                    price_text = price_element.text.strip()
                    # Extract only the numeric part after "PKR"
                    if 'PKR' in price_text:
                        price = price_text.replace('PKR', '').strip()
                    else:
                        price = price_text
                else:
                    price = 'N/A'
                
                # Get original price from strike-through value
                original_price_element = element.select_one(self.config.get('original_price_selector', '.price .strike-through .value'))
                original_price = ''
                if original_price_element:
                    original_price_text = original_price_element.text.strip()
                    # Extract only the numeric part after "PKR"
                    if 'PKR' in original_price_text:
                        original_price = original_price_text.replace('PKR', '').strip()
                    else:
                        original_price = original_price_text
                else:
                    original_price = price
                
                
                # Get category from GTM data if available, unless it's already been determined from product name
                if not item_category or item_category == "":
                    item_category = gtm_data.get('category', category) if gtm_data else category
                else:
                    # Product name-based category takes precedence
                    pass
                
                # For Khaadi, "Fabrics" category is considered "Unstitched"
                if item_category == "Fabrics" or item_category == "fabrics":
                    item_category = "Unstitched"
                
                # Extract color from sku (format usually: CODE_COLOR)
                color = ""
                if sku and "_" in sku:
                    color_part = sku.split("_")[-1]
                    # Convert to title case for consistent formatting
                    color = color_part.title()
                
                
                product = {
                    'brand': 'Khaadi',
                    'brand_id': self.brand_id,
                    'sku': sku,
                    'name': name,
                    'slug': f"{name.lower().replace(' ', '-')}-{brand_info.lower().replace(' | ', '-').replace(' ', '-')}",
                    'product_type': product_type,
                    'category': item_category,
                    'colors': [color.title()] if color else [],
                    'url': product_url,
                }
                
                products.append(product)
                
            except Exception as e:
                self.logger.warning(f"Failed to extract product data: {str(e)}")
        
        return products 

    def get_product_details(self, product_id_or_ids, color=None, size=None, max_workers=5) -> Union[Dict[str, Any], Dict[str, Dict[str, Any]]]:
        """
        Get detailed product information for a single product or batch of products.
        
        Args:
            product_id_or_ids: Either a single product ID (str) or a list of dictionaries with product information.
                               Each dict should have 'id', 'color' and optionally 'size'
            color: Color of the product (for single product requests only)
            size: Size of the product (for single product requests only)
            max_workers: Maximum number of parallel workers for batch processing
            
        Returns:
            For single product: Dictionary containing detailed product information
            For batch: Dictionary mapping product IDs to their detailed information
        """
        # Determine if this is a batch or single product request
        is_batch = isinstance(product_id_or_ids, list)
        
        if is_batch:
            return self._get_product_details_batch(product_id_or_ids, max_workers)
        else:
            return self._get_product_details_single(product_id_or_ids, color, size)
    
    def _get_product_details_single(self, product_id: str, color: str, size: str = None) -> Dict[str, Any]:
        """
        Get detailed product information for a single product.
        
        Args:
            product_id: Product ID (e.g., SS160B-VG_MULTI)
            color: Color of the product (e.g., MULTI)
            size: Size of the product (e.g., 3PC). If None, will be extracted from product_id if possible
            
        Returns:
            Dictionary containing detailed product information
        """
        self.logger.info(f"Getting detailed product information for {product_id}")
        
        try:
            # Parse the product ID
            if "_" in product_id:
                code, pid_color = product_id.split("_", 1)  # Split on first underscore only
            else:
                code = product_id
                pid_color = color
            
            # If size is not provided, try to extract from the product ID or use a default
            if size is None:
                # Check if code contains size information (e.g., SS160B-3PC)
                if "-" in code:
                    # Handle product IDs with multiple hyphens (e.g., 25-03E32-04TB-VG)
                    parts = code.split("-")
                    # Check the last part for size information
                    possible_size = parts[-1]
                    if possible_size in ["2PC", "3PC", "1PC"]:
                        size = possible_size
                        # Reconstruct the base code without the size part
                        base_code = "-".join(parts[:-1])
                    else:
                        # If there are multiple parts, check the second-to-last part
                        if len(parts) > 1 and parts[-2] in ["2PC", "3PC", "1PC"]:
                            size = parts[-2]
                            # Reconstruct base code without the size part
                            parts_without_size = parts.copy()
                            parts_without_size.pop(-2)
                            base_code = "-".join(parts_without_size)
                        else:
                            size = "3PC"  # Default to 3PC if size can't be determined
                            base_code = code
                else:
                    size = "3PC"  # Default size for Khaadi unstitched
                    base_code = code
            else:
                base_code = code  # Use the full code if size is provided externally
            
            # Normalize color case for URL parameters (Khaadi seems to use title case in some cases)
            url_color = color.upper()  # Keep uppercase for consistency
            
            # Construct the API URL
            url = f"{self.base_url}/on/demandware.store/Sites-Khaadi_PK-Site/en_PK/Product-Variation"
            
            # Prepare URL parameters
            params = {
                f"dwvar_{code}__{url_color}_color": url_color,
                f"dwvar_{code}__{url_color}_size": size,
                "pid": product_id,
                "quantity": "1",
                "fromSizeSwatch": "true"
            }
            
            # For debugging
            self.logger.debug(f"Request URL parameters: {params}")
            
            # Additional headers specific to this AJAX request
            headers = {
                "X-Requested-With": "XMLHttpRequest",
                "Accept": "*/*",
                "Referer": f"{self.base_url}/fabrics-3-piece/{product_id}.html",
                "sec-ch-ua": '"Microsoft Edge";v="135", "Not-A.Brand";v="8", "Chromium";v="135"',
                "sec-ch-ua-platform": '"Windows"',
            }
            
            # Make request with the request manager - rotate identity first
            self.request_manager.rotate_identity()
            
            response, success = self.request_manager.make_request(
                url=url,
                params=params,
                headers=headers,
                timeout=self.timeout
            )
            
            if not success:
                self.logger.error(f"Failed to retrieve product details for {product_id} after multiple attempts. Status code: {response.status_code}")
                return {}
            
            # Parse the JSON response
            product_data = json.loads(response.text)
            
            # Get product object
            product = product_data.get("product", {})
            
            # Function to normalize URLs for consistency
            def normalize_url(url):
                # Remove query parameters
                if '?' in url:
                    url = url.split('?')[0]
                
                # Make sure URLs are absolute
                if url and not url.startswith('http'):
                    url = self.base_url + url
                
                return url
            
            # Extract high-res images from the response (removing query parameters)
            hi_res_images = []
            seen_urls = set()  # For deduplication
            
            # Process images by type with preference for 'large' or 'hi-res' images
            image_order_preference = ['large', 'hi-res', 'small']
            
            # Sort image types by preference
            image_types = sorted(
                product.get("images", {}).keys(),
                key=lambda x: image_order_preference.index(x) if x in image_order_preference else 999
            )
            
            # Extract and deduplicate images
            for img_type in image_types:
                images = product.get("images", {}).get(img_type, [])
                for image in images:
                    img_url = image.get("url", "")
                    if img_url:
                        # Normalize the URL for consistency
                        img_url = normalize_url(img_url)
                        
                        # Check if we've seen this image URL before (after normalization)
                        img_path = img_url.split('/')[-1]  # Extract just the filename
                        if img_path not in seen_urls:
                            seen_urls.add(img_path)
                            hi_res_images.append(img_url)
            
            # Extract color variations
            color_variations = []
            size_variations = []
            for variation in product.get("variationAttributes", []):
                if variation.get("id") == "color":
                    color_variations = [value.get("displayValue", "") for value in variation.get("values", [])]
                elif variation.get("id") == "size":
                    size_variations = [self._format_size_value(value.get("displayValue", "")) for value in variation.get("values", [])]
            
            # Extract custom attributes for fabric details
            custom_data = product.get("custom", {})
            fabric_details = {
                "top_fabric": custom_data.get("top_fabric_attribute", ""),
                "bottom_fabric": custom_data.get("bottom_fabric_attribute", ""),
                "dupatta_fabric": custom_data.get("dupatta_fabric_attribute", ""),
                "main_fabric": custom_data.get("main_fabric_attribute", ""),
                "material": custom_data.get("material", ""),
                "technique": custom_data.get("technique", "")
            }
            
            # Extract additional product attributes
            launch_date = custom_data.get("launch", "") if custom_data else ""
            # Process launch date to extract month
            launch_month = self._process_launch_date(launch_date)
            product_concept = custom_data.get("productConcept", "") if custom_data else ""
            season = custom_data.get("season_attribute", "") if custom_data else ""
            
            # Combine month and season into collection
            collection = self._combine_season_info(launch_month, season)
            
            # Create sortable launch time
            launch_time = self._create_launch_time(launch_date, launch_month, season)
            
            # Extract detailed price information
            price_info = product.get("price", {})
            sales_data = price_info.get("sales", {}) if price_info else {}
            list_data = price_info.get("list", {}) if price_info else {}
            
            # Get current and original price values
            current_price_value = sales_data.get("value", 0) if sales_data else 0
            original_price_value = list_data.get("value", 0) if list_data else 0
            
            # If no original price is provided, use current price as original
            if original_price_value == 0:
                original_price_value = current_price_value
            
            # Determine if the product is on sale based on price comparison
            is_on_sale = current_price_value < original_price_value
            
            # Calculate discount percentage if there's a difference between prices
            discount_percentage = 0
            if original_price_value > 0 and original_price_value != current_price_value:
                discount_percentage = round(((original_price_value - current_price_value) / original_price_value) * 100, 2)
            
            # Extract availability information
            in_stock = product.get("available", False)
            
            # If we have specific status information, use it to determine in_stock
            if product.get("availableStatus", "") == "IN_STOCK":
                in_stock = True
            elif product.get("availableStatus", "") == "OUT_OF_STOCK":
                in_stock = False
            
            # Extract remaining stock quantity from maxOrderQuantity
            remaining_stock = product.get("maxOrderQuantity", 0)
            
            # Extract product metadata
            metadata = {
                "is_new": product.get("isNew", False),
                "is_sale": is_on_sale,
                "product_name": product.get("productName", ""),
                "product_type": product.get("productType", ""),
                "short_description": product.get("shortDescription", ""),
                "long_description": product.get("longDescription", ""),
                "rating": product.get("rating", 0)
            }
            
            # Extract GTM data for additional info
            gtm_data = product.get("gtmData", {})
            gtm_category = gtm_data.get("category", "")
            
            # Determine category from product name if available
            product_name = metadata["product_name"]
            if product_name:
                if "Fabrics" in product_name:
                    gtm_category = "Unstitched"
                elif "Ready" in product_name or "RTW" in product_name:
                    gtm_category = "Ready to Wear"
            
            # For Khaadi, "Fabrics" category is considered "Unstitched"
            if gtm_category == "Fabrics" or gtm_category == "fabrics":
                gtm_category = "Unstitched"
            
            # Build the complete detailed product object
            detailed_product = {
                "sku": product_id,
                "name": metadata["product_name"],
                "product_type": metadata["product_type"],
                "category": gtm_category,
                "short_description": self._convert_to_array(metadata["short_description"]),
                "long_description": metadata["long_description"],
                "rating": metadata["rating"],
                "is_new": metadata["is_new"],
                "is_sale": is_on_sale,
                "price_current": current_price_value,
                "price_original": original_price_value,
                "discount_percentage": discount_percentage,
                "images": hi_res_images,
                "colors": color_variations,
                "sizes": size_variations,
                "in_stock": in_stock,
                "remaining_stock": remaining_stock,
                "fabric_details": fabric_details,
                "collection": collection,
                "launch_time": launch_time,
                "product_concept": product_concept,
                "metadata": metadata,
                "raw_data": product_data  # Include full raw data for debugging
            }
            
            # Convert string fields with multiple words into arrays
            for field in ['material', 'technique', 'top_fabric', 
                         'bottom_fabric', 'dupatta_fabric', 'main_fabric']:
                if field in detailed_product and detailed_product[field]:
                    detailed_product[field] = self._convert_to_array(detailed_product[field]) # type: ignore
            
            return detailed_product
        
        except Exception as e:
            self.logger.error(f"Error getting product details for {product_id}: {str(e)}")
            # Print a traceback for better debugging
            import traceback
            self.logger.error(f"Traceback: {traceback.format_exc()}")
            return {}
    
    def _get_product_details_batch(self, product_ids: List[Dict[str, str]], max_workers: int = 5) -> Dict[str, Dict[str, Any]]:
        """
        Get detailed product information for multiple products in parallel.
        
        Args:
            product_ids: List of dictionaries with product information. Each dict should have 'id', 'color' and optionally 'size'
            max_workers: Maximum number of parallel workers
            
        Returns:
            Dictionary mapping product IDs to their detailed information
        """
        self.logger.info(f"Getting detailed product information for {len(product_ids)} products in parallel")
        
        results = {}
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(max_workers, len(product_ids))) as executor:
            futures = {}
            
            for product in product_ids:
                product_id = product.get('id')
                color = product.get('color')
                size = product.get('size', None)
                
                if not product_id or not color:
                    self.logger.warning(f"Skipping product with missing ID or color: {product}")
                    continue
                
                try:
                    # Create a dedicated request manager for each thread to avoid conflicts
                    request_manager_copy = RequestManager(
                        base_url=self.base_url,
                        retry_count=self.retry_count,
                        retry_delay=self.retry_delay
                    )
                    
                    # Explicitly rotate identity before each product request
                    request_manager_copy.rotate_identity()
                    
                    # Replace the request manager temporarily for this product
                    original_manager = self.request_manager
                    self.request_manager = request_manager_copy
                    
                    future = executor.submit(self._get_product_details_single, product_id, color, size)
                    futures[future] = product_id
                    
                    # Restore the original request manager
                    self.request_manager = original_manager
                except Exception as e:
                    self.logger.error(f"Error setting up request for product {product_id}: {str(e)}")
                    import traceback
                    self.logger.error(f"Traceback: {traceback.format_exc()}")
            
            for future in concurrent.futures.as_completed(futures):
                product_id = futures[future]
                try:
                    result = future.result()
                    if result:
                        results[product_id] = result
                        self.logger.info(f"Successfully retrieved details for {product_id}")
                    else:
                        self.logger.warning(f"No data returned for product {product_id}")
                        results[product_id] = {}
                except Exception as e:
                    self.logger.error(f"Failed to get details for {product_id}: {str(e)}")
                    import traceback
                    self.logger.error(f"Traceback: {traceback.format_exc()}")
                    results[product_id] = {}
                
                # Add a small delay between requests to avoid rate limiting
                time.sleep(random.uniform(0.2, 0.5))
        
        return results

    def enhance_products_with_details(self, products: List[Dict[str, Any]], max_workers: int = 3) -> List[Dict[str, Any]]:
        """
        Enhance product information by fetching detailed data for each product.
        
        Args:
            products: List of products from the regular scraping process
            max_workers: Maximum number of parallel workers
            
        Returns:
            List of enhanced product dictionaries
        """
        self.logger.info(f"Enhancing {len(products)} products with detailed information")
        
        # Extract product IDs, colors and sizes for batch processing
        product_requests = []
        for product in products:
            try:
                sku = product.get('sku', '')
                color = product.get('colors', [])[0] if product.get('colors') else ''
                
                # Skip products without SKU or color
                if not sku or not color:
                    self.logger.warning(f"Skipping product without SKU or color: {product.get('name', 'Unknown')}")
                    continue
                    
                # Determine the size from product type or use None (will be detected in get_product_details)
                size = None
                product_type = product.get('product_type', '')
                if '3 Piece' in product_type:
                    size = '3PC'
                elif '2 Piece' in product_type:
                    size = '2PC'
                
                product_requests.append({
                    'id': sku,
                    'color': color,
                    'size': size
                })
                
            except Exception as e:
                self.logger.error(f"Error preparing product request: {str(e)}")
                import traceback
                self.logger.error(f"Traceback: {traceback.format_exc()}")
        
        # Get detailed information for all products
        self.logger.info(f"Requesting details for {len(product_requests)} products")
        details_map = self.get_product_details(product_requests, max_workers)
        self.logger.info(f"Received details for {len(details_map)} products")
        
        # Enhance products with detailed information
        enhanced_products = []
        for product in products:
            try:
                sku = product.get('sku', '')
                
                if sku in details_map:
                    details = details_map[sku]
                    
                    # Only enhance product if we successfully got details
                    if details:
                        # Get current and original price values for sale determination
                        current_price = details.get('price_current', 0)
                        original_price = details.get('price_original', 0)
                        
                        # Try alternate approach if price data is not found in expected structure
                        if current_price == 0:
                            # If still 0, try to get from raw data
                            if 'raw_data' in details:
                                try:
                                    raw_product = details.get('raw_data', {}).get('product', {})
                                    raw_price = raw_product.get('price', {})
                                    raw_sales = raw_price.get('sales', {})
                                    raw_list = raw_price.get('list', {})
                                    
                                    if raw_sales and 'value' in raw_sales:
                                        current_price = raw_sales.get('value', 0)
                                    
                                    if raw_list and 'value' in raw_list:
                                        original_price = raw_list.get('value', 0)
                                except Exception as e:
                                    self.logger.warning(f"Error extracting price from raw data: {str(e)}")
                        
                        # Log price info for debugging
                        self.logger.debug(f"Price info for {sku}: current={current_price}, original={original_price}")
                        
                        # Calculate if the product is on sale based on price comparison 
                        if original_price == 0:
                            original_price = current_price
                        # rather than using the original is_sale flag
                        is_on_sale = current_price < original_price and current_price > 0
                        
                        # Calculate discount percentage
                        discount_percentage = 0
                        if original_price > 0 and original_price != current_price:
                            discount_percentage = round(((original_price - current_price) / original_price) * 100, 2)
                        
                        # Get enhanced product data from the details
                        product.update({
                            # Product attributes
                            'name': details.get('name', product.get('name', '')),
                            'short_description': self._convert_to_array(details.get('short_description', '')),
                            'long_description': details.get('long_description', ''),
                            'rating': details.get('rating', 0),
                            'is_new': details.get('is_new', False),
                            'is_sale': is_on_sale,  # Use calculated value instead of the original flag
                            
                            # Fabric details
                            'top_fabric': self._convert_to_array(details.get('fabric_details', {}).get('top_fabric', '')),
                            'bottom_fabric': self._convert_to_array(details.get('fabric_details', {}).get('bottom_fabric', '')),
                            'dupatta_fabric': self._convert_to_array(details.get('fabric_details', {}).get('dupatta_fabric', '')),
                            'main_fabric': self._convert_to_array(details.get('fabric_details', {}).get('main_fabric', '')),
                            'material': self._convert_to_array(details.get('fabric_details', {}).get('material', '')),
                            'technique': self._convert_to_array(details.get('fabric_details', {}).get('technique', '')),
                            
                            # Variations
                            'colors': details.get('colors', []),
                            'sizes': details.get('sizes', []),
                            
                            # Additional product attributes
                            'collection': details.get('collection', self._combine_season_info(
                                self._process_launch_date(details.get('launch_date', '')),
                                details.get('season', '')
                            )),
                            'launch_time': details.get('launch_time', self._create_launch_time(
                                details.get('launch_date', ''),
                                self._process_launch_date(details.get('launch_date', '')),
                                details.get('season', '')
                            )),
                            'product_concept': details.get('product_concept', ''),
                            
                            # Price information (already in the new format)
                            'price_current': current_price,  # Use the values we extracted and calculated
                            'price_original': original_price,  # instead of getting them from details
                            'discount_percentage': discount_percentage,
                            
                            # Availability information
                            'in_stock': details.get('in_stock', product.get('in_stock', True)),
                            'remaining_stock': details.get('remaining_stock', 0),
                            
                            # Flag to indicate product has detailed info
                            'has_detailed_info': True
                        })
                        
                        # Store product images
                        product['img_url'] = details.get('images', [])
                        
                        # Update category based on product name if available
                        product_name = product.get('name', '')
                        if product_name:
                            if "Fabrics" in product_name:
                                product['category'] = "Unstitched"
                            elif "Ready" in product_name or "RTW" in product_name:
                                product['category'] = "Ready to Wear"
                        
                        # Ensure "Fabrics" category is always mapped to "Unstitched"
                        if product.get('category') == "Fabrics" or product.get('category') == "fabrics":
                            product['category'] = "Unstitched"
                        
                        self.logger.debug(f"Enhanced product {sku} with detailed information")
                    else:
                        self.logger.warning(f"No details found for product {sku}, couldn't enhance")
                else:
                    self.logger.warning(f"Product {sku} not found in details map, couldn't enhance")
            except Exception as e:
                self.logger.error(f"Error enhancing product {product.get('sku', 'Unknown')}: {str(e)}")
                import traceback
                self.logger.error(f"Traceback: {traceback.format_exc()}")
            
            enhanced_products.append(product)
        
        # Log statistics about enhancement
        detailed_count = sum(1 for p in enhanced_products if p.get('has_detailed_info', False))
        self.logger.info(f"Enhanced {detailed_count} out of {len(enhanced_products)} products with detailed information")
        
        return enhanced_products 

    def _format_size_value(self, size_value):
        """
        Format size values: convert numeric sizes like "008" to "8" while leaving non-numeric sizes like "3PC" unchanged.
        
        Args:
            size_value: Original size value as string
            
        Returns:
            Formatted size value
        """
        try:
            # If the size is purely numeric, remove leading zeros
            if size_value.isdigit():
                return str(int(size_value))
            else:
                return size_value
        except (ValueError, AttributeError):
            # If any error occurs, return the original value
            return size_value
            
    def _process_launch_date(self, launch_date_str):
        """
        Process launch date and extract only the month as a numeric value.
        
        Handles various formats:
        - "09. SEPTEMBER" -> 9
        - "25-03" -> 3
        - "09-09" -> 9
        
        Args:
            launch_date_str: Original launch date string
            
        Returns:
            Integer representing the month (1-12), or None if cannot be determined
        """
        if not launch_date_str:
            return None
            
        try:
            # Format: "09. SEPTEMBER"
            if '.' in launch_date_str and len(launch_date_str.split('.')) > 1:
                month_num = launch_date_str.split('.')[0].strip()
                return int(month_num)
                
            # Format: "25-03" or "09-09"
            elif '-' in launch_date_str and len(launch_date_str.split('-')) > 1:
                month_part = launch_date_str.split('-')[1].strip()
                return int(month_part)
                
            # Try to directly parse as integer (if it's just a month number)
            else:
                return int(launch_date_str)
                
        except (ValueError, IndexError, AttributeError):
            # Log the unparseable format but don't raise an exception
            self.logger.warning(f"Could not parse launch date format: {launch_date_str}")
            return None
            
    def _combine_season_info(self, month, season):
        """
        Combine month and season information into a single collection attribute.
        
        Args:
            month: Numeric month (1-12)
            season: Season text (e.g., 'SUMMER', 'WINTER', etc.)
            
        Returns:
            Collection information formatted as "Season Year" (e.g., "Fall 2024")
        """
        import datetime
        
        # Get current year as default
        current_year = datetime.datetime.now().year
        
        # Convert month to season name if available
        month_to_season = {
            12: "Winter", 1: "Winter", 2: "Winter",
            3: "Spring", 4: "Spring", 5: "Spring",
            6: "Summer", 7: "Summer", 8: "Summer",
            9: "Fall", 10: "Fall", 11: "Fall"
        }
        
        # Get natural season based on month
        natural_season = month_to_season.get(month, "") if month else ""
        
        # Format the brand's specified season
        formatted_season = season.strip().title() if season else ""
        
        # Choose the season to display
        display_season = ""
        if formatted_season and natural_season:
            if formatted_season.lower() in ["summer", "winter", "spring", "fall", "autumn"]:
                display_season = formatted_season
            else:
                display_season = natural_season
        elif formatted_season:
            display_season = formatted_season
        elif natural_season:
            display_season = natural_season
        else:
            return "Collection"  # Default if no season information is available
        
        # Standardize "Fall" vs "Autumn"
        if display_season.lower() == "autumn":
            display_season = "Fall"
            
        # Format as "Season Year"
        return f"{display_season} {current_year}"
        
    def _create_launch_time(self, launch_date_str, month=None, season=None):
        """
        Create a standardized launch time for sorting products chronologically.
        Returns YYYY-MM format string for simple sorting.
        
        Args:
            launch_date_str: Original launch date string
            month: Numeric month (1-12) if already extracted
            season: Season text for additional context
            
        Returns:
            YYYY-MM format string for chronological sorting
        """
        import datetime
        
        # Get current year and month as defaults
        current_year = datetime.datetime.now().year
        current_month = datetime.datetime.now().month
        
        # Try to extract month from launch_date_str if not provided
        if not month and launch_date_str:
            month = self._process_launch_date(launch_date_str)
        
        # If we have a month, use it; otherwise use current month
        month_to_use = month if month else current_month
        
        # Determine year: if current month is later than product month, likely product is from previous year
        year_to_use = current_year
        if current_month > month_to_use:
            # Product is likely from current year
            pass
        else:
            # If we're in January and product is from December, it's likely from previous year
            if current_month <= 3 and month_to_use >= 10:
                year_to_use = current_year - 1
        
        # Create YYYY-MM formatted string
        try:
            # Format with zero-padding for month
            return f"{year_to_use}-{month_to_use:02d}"
        except (ValueError, TypeError):
            # If any error in creating the format, return current year-month
            self.logger.warning(f"Could not create launch time for date: {launch_date_str}, month: {month}, season: {season}")
            return f"{current_year}-{current_month:02d}" 