import argparse
import sys
import os
from dotenv import load_dotenv
from typing import List, Optional, Dict, Any
import json
from datetime import datetime

from supabase import create_client, Client

from src.scraper_factory import ScraperFactory
from src.utils.config_loader import ConfigLoader
from src.utils.data_processor import DataProcessor

# Load environment variables from .env file
load_dotenv()

# Initialize Supabase client
supabase_url = os.environ.get("SUPABASE_URL")
supabase_key = os.environ.get("SUPABASE_KEY")
supabase: Client = create_client(supabase_url, supabase_key)

def fetch_brands_from_supabase() -> List[Dict[str, Any]]:
    """
    Fetch brands from Supabase brand table.
    
    Returns:
        List of dictionaries containing brand data (id, name, logo_url)
    """
    try:
        response = supabase.table('brand').select('id, name, logo_url').execute()
        return response.data
    except Exception as e:
        print(f"Error fetching brands from Supabase: {str(e)}")
        return []

def run_scraper(brand_name: str, brand_id: Optional[int] = None, output_format: str = 'json', 
                use_supabase: bool = False, all_data: Optional[List[Dict[str, Any]]] = None) -> List[Dict[str, Any]]:
    """
    Run scraper for a specific brand.
    
    Args:
        brand_name: Name of the brand to scrape
        brand_id: ID of the brand in the Supabase database
        output_format: Format to save the data in (json or csv)
        use_supabase: Whether to save data to Supabase
        all_data: List to append data to when scraping all brands
        
    Returns:
        List of scraped product data
    """
    print(f"Starting scraper for {brand_name}...")
    
    # Create scraper
    scraper = ScraperFactory.create_scraper(brand_name)
    if not scraper:
        print(f"Failed to create scraper for {brand_name}")
        return []
    
    try:
        # Run scraper
        data = scraper.scrape()
        
        if not data:
            print(f"No data found for {brand_name}")
            return []
        
        print(f"Scraped {len(data)} items from {brand_name}")
        
        # Add brand_id to each item if provided
        if brand_id is not None:
            for item in data:
                # Create a new dictionary with brand_id first, then all other items
                new_item = {"brand_id": brand_id}
                # Add all other items while excluding 'brand' if it exists
                for key, value in item.items():
                    if key != 'brand':
                        new_item[key] = value
                # Replace the original item with the new ordered one
                for i in range(len(data)):
                    if data[i] is item:
                        data[i] = new_item
        
        # Append data to all_data if provided
        if all_data is not None:
            all_data.extend(data)
        
        # Save data to file if not collecting all data in a single file
        if all_data is None:
            if output_format.lower() == 'json':
                output_file = DataProcessor.save_to_json(data, brand_name)
                print(f"Data saved to {output_file}")
            elif output_format.lower() == 'csv':
                output_file = DataProcessor.save_to_csv(data, brand_name)
                print(f"Data saved to {output_file}")
            else:
                print(f"Unsupported output format: {output_format}")
        
        # Save data to Supabase
        if use_supabase:
            try:
                # Insert data into 'product_listing' table
                formatted_data = []
                for item in data:
                    # Convert item to proper format, ensuring raw_data is properly handled
                    formatted_item = {
                        # Identification fields
                        "brand_id": item.get('brand_id', brand_id),
                        "sku": item.get('sku', ''),
                        "name": item.get('name', ''),
                        
                        # Product classification
                        "product_type": item.get('product_type', ''),
                        "category": item.get('category', ''),
                        
                        # Collection and piece information
                        "collection": item.get('collection', ''),
                        "piece_info": item.get('piece_info', ''),
                        
                        # Fabric details
                        "fabric_slug": item.get('fabric_slug', ''),
                        "fabric_style": item.get('fabric_style', ''),
                        "fabric_type": item.get('fabric_type', ''),
                        "color": item.get('color', ''),
                        
                        # URLs
                        "url": item.get('url', ''),
                        
                        # Images
                        "primary_image_url": item.get('primary_image_url', ''),
                        "hover_image_url": item.get('hover_image_url', ''),
                        "image_url": item.get('primary_image_url', ''),  # For backward compatibility
                        
                        # Pricing
                        "price": str(item.get('price', '')),
                        "original_price": str(item.get('original_price', '')),
                        "discount_percentage": item.get('discount_percentage'),
                        
                        # Status
                        "on_sale": item.get('on_sale', False),
                        "in_stock": item.get('in_stock', True),
                        
                        # Metadata
                        "raw_data": item
                    }
                    formatted_data.append(formatted_item)
                
                result = supabase.table('product_listing').insert(formatted_data).execute()
                
                print(f"Data successfully saved to Supabase ({len(data)} items)")
            except Exception as e:
                print(f"Error saving data to Supabase: {str(e)}")
        
        return data
            
    except Exception as e:
        print(f"Error running scraper for {brand_name}: {str(e)}")
        return []

def list_available_brands() -> None:
    """List available brands for scraping."""
    print("Available brands for scraping:")
    brands = ConfigLoader.get_available_brands()
    
    if not brands:
        print("No brands available")
        return
    
    for i, brand in enumerate(brands, 1):
        print(f"{i}. {brand}")

def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Web scraper for multiple brands')
    
    parser.add_argument('--brand', type=str, help='Brand to scrape')
    parser.add_argument('--all', action='store_true', help='Scrape all available brands')
    parser.add_argument('--list', action='store_true', help='List available brands for scraping')
    parser.add_argument('--format', type=str, choices=['json', 'csv'], default='json',
                        help='Output format (json or csv)')
    parser.add_argument('--supabase', action='store_true', help='Save data to Supabase')
    
    return parser.parse_args()

def main() -> int:
    """Main entry point."""
    args = parse_args()
    
    # Check if Supabase environment variables are set when the flag is used
    if args.supabase and (not supabase_url or not supabase_key):
        print("Error: Supabase URL or key not found in environment variables.")
        print("Make sure the .env file exists and contains SUPABASE_URL and SUPABASE_KEY.")
        return 1
    
    if args.list:
        list_available_brands()
        return 0
    
    if args.all:
        # Fetch brands from Supabase
        db_brands = fetch_brands_from_supabase()
        
        # Create brand mapping {name: id}
        brand_map = {brand['name'].lower(): brand['id'] for brand in db_brands}
        
        # Get available brands from config
        brands = ConfigLoader.get_available_brands()
        if not brands:
            print("No brands available for scraping")
            return 1
        
        # Collect all data in a single list
        all_data = []
        
        for brand in brands:
            # Get brand_id from mapping if available
            brand_id = brand_map.get(brand.lower())
            
            # Run scraper with all_data to collect results
            run_scraper(brand, brand_id, args.format, args.supabase, all_data)
        
        # Save all combined data to a single file
        if all_data:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            if args.format.lower() == 'json':
                output_file = f"data/all_brands_{timestamp}.json"
                os.makedirs(os.path.dirname(output_file), exist_ok=True)
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump(all_data, f, ensure_ascii=False, indent=2)
                print(f"Combined data for all brands saved to {output_file}")
            elif args.format.lower() == 'csv':
                output_file = f"data/all_brands_{timestamp}.csv"
                DataProcessor.save_list_to_csv(all_data, output_file)
                print(f"Combined data for all brands saved to {output_file}")
            else:
                print(f"Unsupported output format: {args.format}")
        
        return 0
    
    if not args.brand:
        print("Please specify a brand to scrape or use --all to scrape all brands")
        print("Use --list to see available brands")
        return 1
    
    # Fetch brand_id from Supabase if using Supabase
    brand_id = None
    if args.supabase:
        db_brands = fetch_brands_from_supabase()
        brand_map = {brand['name'].lower(): brand['id'] for brand in db_brands}
        brand_id = brand_map.get(args.brand.lower())
    
    run_scraper(args.brand, brand_id, args.format, args.supabase)
    return 0

if __name__ == '__main__':
    sys.exit(main()) 