from typing import Any, Dict, List, Optional
import json
import os
from datetime import datetime

class DatabaseManager:
    """
    Manager for database operations related to scraped data.
    
    This is a placeholder implementation that uses local JSON files for storage
    instead of a remote database. For production use, consider implementing
    a proper database connection.
    """
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize the database manager.
        
        Args:
            config_path: Path to configuration file (not used in this implementation)
        """
        self.data_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'data')
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)
    
    def save_scraped_data(self, brand_name: str, data: List[Dict[str, Any]]) -> bool:
        """
        Save scraped data to a local JSON file.
        
        Args:
            brand_name: Name of the brand
            data: List of scraped data items
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # First, save scrape metadata
            scrape_id = self._save_scrape_metadata(brand_name, len(data))
            
            # Save the items to a JSON file
            filename = f"{brand_name.lower().replace(' ', '_')}_{scrape_id}.json"
            filepath = os.path.join(self.data_dir, filename)
            
            items_data = []
            for item in data:
                # Extract key product information
                items_data.append({
                    "scrape_id": scrape_id,
                    "brand": brand_name,
                    "sku": item.get('sku', ''),
                    "name": item.get('name', ''),
                    "product_type": item.get('product_type', ''),
                    "fabric_style": item.get('fabric_style', ''),
                    "fabric_type": item.get('fabric_type', ''),
                    "category": item.get('category', ''),
                    "color": item.get('color', ''),
                    "url": item.get('url', ''),
                    "primary_image_url": item.get('primary_image_url', ''),
                    "hover_image_url": item.get('hover_image_url', ''),
                    "price": item.get('price', ''),
                    "original_price": item.get('original_price', ''),
                    "discount_percentage": item.get('discount_percentage', ''),
                    "status": item.get('status', ''),
                    "scraped_data": item,
                    "created_at": datetime.utcnow().isoformat()
                })
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(items_data, f, ensure_ascii=False, indent=4)
            
            return True
        except Exception as e:
            print(f"Error saving scraped data: {str(e)}")
            return False
    
    def _save_scrape_metadata(self, brand_name: str, item_count: int) -> str:
        """
        Save metadata about a scrape operation.
        
        Args:
            brand_name: Name of the brand
            item_count: Number of items scraped
            
        Returns:
            ID of the created metadata record
        """
        timestamp = datetime.utcnow().isoformat()
        scrape_id = f"{int(datetime.utcnow().timestamp())}"
        
        metadata = {
            "id": scrape_id,
            "brand": brand_name,
            "item_count": item_count,
            "scraped_at": timestamp,
            "status": "completed"
        }
        
        metadata_file = os.path.join(self.data_dir, "scrape_metadata.json")
        
        # Load existing metadata if it exists
        all_metadata = []
        if os.path.exists(metadata_file):
            try:
                with open(metadata_file, 'r', encoding='utf-8') as f:
                    all_metadata = json.load(f)
            except json.JSONDecodeError:
                all_metadata = []
        
        all_metadata.append(metadata)
        
        # Save updated metadata
        with open(metadata_file, 'w', encoding='utf-8') as f:
            json.dump(all_metadata, f, ensure_ascii=False, indent=4)
        
        return scrape_id
    
    def get_latest_scrape(self, brand_name: Optional[str] = None) -> Dict[str, Any]:
        """
        Get the latest scrape metadata.
        
        Args:
            brand_name: Optional brand name to filter by
            
        Returns:
            Latest scrape metadata
        """
        metadata_file = os.path.join(self.data_dir, "scrape_metadata.json")
        
        if not os.path.exists(metadata_file):
            return {}
        
        try:
            with open(metadata_file, 'r', encoding='utf-8') as f:
                all_metadata = json.load(f)
            
            # Filter by brand if specified
            if brand_name:
                filtered_metadata = [m for m in all_metadata if m.get("brand") == brand_name]
            else:
                filtered_metadata = all_metadata
            
            # Sort by scraped_at in descending order
            sorted_metadata = sorted(filtered_metadata, key=lambda x: x.get("scraped_at", ""), reverse=True)
            
            if sorted_metadata:
                return sorted_metadata[0]
        except Exception as e:
            print(f"Error getting latest scrape: {str(e)}")
        
        return {}
    
    def get_scrape_items(self, scrape_id: str) -> List[Dict[str, Any]]:
        """
        Get items from a specific scrape operation.
        
        Args:
            scrape_id: ID of the scrape operation
            
        Returns:
            List of scraped items
        """
        # First, find the brand for this scrape ID
        metadata_file = os.path.join(self.data_dir, "scrape_metadata.json")
        
        if not os.path.exists(metadata_file):
            return []
        
        brand_name = ""
        try:
            with open(metadata_file, 'r', encoding='utf-8') as f:
                all_metadata = json.load(f)
            
            for metadata in all_metadata:
                if metadata.get("id") == scrape_id:
                    brand_name = metadata.get("brand", "")
                    break
        except Exception:
            pass
        
        if not brand_name:
            return []
        
        # Now find all files that might contain this scrape ID
        items = []
        filename = f"{brand_name.lower().replace(' ', '_')}_{scrape_id}.json"
        filepath = os.path.join(self.data_dir, filename)
        
        if os.path.exists(filepath):
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    items = json.load(f)
                    
                # Extract the actual data from scraped_data field
                processed_items = []
                for item in items:
                    if isinstance(item.get("scraped_data"), dict):
                        data = item["scraped_data"]
                        data["id"] = item.get("id", "")
                        data["created_at"] = item.get("created_at", "")
                        processed_items.append(data)
                    else:
                        processed_items.append(item)
                
                return processed_items
            except Exception as e:
                print(f"Error reading scrape items: {str(e)}")
        
        return []
    
    def get_items_by_brand(self, brand_name: str, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get scraped items for a specific brand.
        
        Args:
            brand_name: Name of the brand
            limit: Maximum number of items to retrieve
            
        Returns:
            List of scraped items
        """
        # First get the latest scrape for this brand
        latest_scrape = self.get_latest_scrape(brand_name)
        
        if not latest_scrape:
            return []
        
        scrape_id = latest_scrape.get("id")
        if not scrape_id:
            return []
        
        # Now get the items for this scrape
        items = self.get_scrape_items(scrape_id)
        
        # Apply limit
        return items[:limit]
    
    def get_items_by_category(self, brand_name: str, category: str, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get scraped items for a specific brand and category.
        
        Args:
            brand_name: Name of the brand
            category: Product category
            limit: Maximum number of items to retrieve
            
        Returns:
            List of scraped items
        """
        items = self.get_items_by_brand(brand_name)
        
        # Filter by category
        filtered_items = [item for item in items if item.get("category") == category]
        
        # Apply limit
        return filtered_items[:limit]
    
    def get_items_by_status(self, brand_name: str, status: str, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get scraped items for a specific brand with a particular status (e.g., 'Sale').
        
        Args:
            brand_name: Name of the brand
            status: Product status (e.g., 'Sale', 'In Stock')
            limit: Maximum number of items to retrieve
            
        Returns:
            List of scraped items
        """
        items = self.get_items_by_brand(brand_name)
        
        # Filter by status
        filtered_items = [item for item in items if item.get("status") == status]
        
        # Apply limit
        return filtered_items[:limit]
    
    def delete_scrape_data(self, scrape_id: str) -> bool:
        """
        Delete a scrape operation and its data.
        
        Args:
            scrape_id: ID of the scrape operation
            
        Returns:
            True if successful, False otherwise
        """
        # First, find the brand for this scrape ID
        metadata_file = os.path.join(self.data_dir, "scrape_metadata.json")
        
        if not os.path.exists(metadata_file):
            return False
        
        brand_name = ""
        all_metadata = []
        try:
            with open(metadata_file, 'r', encoding='utf-8') as f:
                all_metadata = json.load(f)
            
            for i, metadata in enumerate(all_metadata):
                if metadata.get("id") == scrape_id:
                    brand_name = metadata.get("brand", "")
                    # Remove this metadata entry
                    all_metadata.pop(i)
                    break
                    
            # Save updated metadata
            with open(metadata_file, 'w', encoding='utf-8') as f:
                json.dump(all_metadata, f, ensure_ascii=False, indent=4)
        except Exception as e:
            print(f"Error updating metadata: {str(e)}")
            return False
        
        if not brand_name:
            return False
        
        # Now delete the data file
        filename = f"{brand_name.lower().replace(' ', '_')}_{scrape_id}.json"
        filepath = os.path.join(self.data_dir, filename)
        
        if os.path.exists(filepath):
            try:
                os.remove(filepath)
                return True
            except Exception as e:
                print(f"Error deleting file: {str(e)}")
        
        return False 