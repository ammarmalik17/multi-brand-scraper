import os
import sys
import json
import socket
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv
from supabase import create_client, Client
from datetime import datetime

def load_env_file():
    """
    Explicitly load the .env file from the current directory.
    
    Returns:
        True if .env file was found and loaded, False otherwise
    """
    # Get current directory path
    current_dir = os.path.dirname(os.path.abspath(__file__))
    env_path = os.path.join(current_dir, '.env')
    
    # Check if .env file exists
    if os.path.isfile(env_path):
        # Load the .env file
        load_dotenv(env_path)
        print(f"Loaded .env file from: {env_path}")
        return True
    else:
        print(f"ERROR: .env file not found at: {env_path}")
        print("Please create a .env file with your Supabase credentials:")
        print("SUPABASE_URL=your_project_url")
        print("SUPABASE_KEY=your_api_key")
        return False

def load_scraped_data(filepath: str) -> List[Dict[str, Any]]:
    """
    Load scraped data from a JSON file.
    
    Args:
        filepath: Path to the JSON file
        
    Returns:
        List of dictionaries containing product data
    """
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return data
    except Exception as e:
        print(f"Error loading data from {filepath}: {str(e)}")
        return []

def format_product_for_supabase(product: Dict[str, Any], brand_id: Optional[str] = None) -> Dict[str, Any]:
    """
    Format a product for insertion into Supabase product table.
    Maps fields from Khaadi scraper to Supabase table schema.
    
    Args:
        product: Dictionary containing product data
        brand_id: Optional UUID for the brand
        
    Returns:
        Dictionary containing formatted product data
    """
    # Get the product_type field and map it to type field
    product_type = product.get('product_type', None)
    
    # Map fields according to the product table schema
    return {
        "sku": product.get('sku', None),
        "name": product.get('name', None),
        "brand_id": brand_id,  # Use provided brand_id
        "url": product.get('url', None),
        "slug": product.get('slug', None),
        "type": product_type,  # Renamed from product_type to type
        "category": product.get('category', None),
        "colors": product.get('colors', None),
        "sizes": product.get('sizes', None),
        "short_description": product.get('short_description', None),
        "long_description": product.get('long_description', None),
        "collection": product.get('collection', None),
        "rating": product.get('rating', None),
        "is_new": product.get('is_new', None),
        "is_sale": product.get('is_sale', None),
        "material": product.get('material', None),
        "technique": product.get('technique', None),
        "img_url": product.get('img_url', None),
        "has_detaild_info": product.get('has_detailed_info', None),
    }

def check_internet_connection(host="8.8.8.8", port=53, timeout=3):
    """
    Check if there is an internet connection by trying to connect to Google's DNS.
    
    Returns:
        True if connected, False otherwise
    """
    try:
        socket.setdefaulttimeout(timeout)
        socket.socket(socket.AF_INET, socket.SOCK_STREAM).connect((host, port))
        return True
    except socket.error:
        return False

def check_supabase_connection(supabase_url):
    """
    Check if the Supabase URL is reachable.
    
    Args:
        supabase_url: Supabase URL to check
        
    Returns:
        True if reachable, False otherwise
    """
    try:
        # Extract hostname from URL
        from urllib.parse import urlparse
        parsed_url = urlparse(supabase_url)
        hostname = parsed_url.netloc
        
        # Try to resolve the hostname
        socket.gethostbyname(hostname)
        return True
    except Exception:
        return False

def upload_to_supabase(data: List[Dict[str, Any]], brand_id: Optional[str] = None) -> bool:
    """
    Upload product data to Supabase.
    
    Args:
        data: List of dictionaries containing product data
        brand_id: Optional UUID for the brand
        
    Returns:
        True if successful, False otherwise
    """
    # Load environment variables from .env file
    if not load_env_file():
        return False
    
    # Get Supabase credentials
    supabase_url = os.environ.get("SUPABASE_URL")
    supabase_key = os.environ.get("SUPABASE_KEY")
    
    # Print the loaded values for debugging (don't show full key)
    if supabase_url:
        print(f"Found SUPABASE_URL: {supabase_url}")
    if supabase_key:
        key_preview = supabase_key[:5] + '...' if len(supabase_key) > 5 else '...'
        print(f"Found SUPABASE_KEY: {key_preview}")
    
    if not supabase_url or not supabase_key:
        print("ERROR: Missing Supabase credentials in .env file")
        print("Please ensure your .env file contains:")
        print("SUPABASE_URL=your_project_url")
        print("SUPABASE_KEY=your_api_key")
        return False
    
    # Check internet connection
    if not check_internet_connection():
        print("ERROR: No internet connection detected")
        print("Please check your network connection and try again")
        return False
    
    # Check if Supabase URL is reachable
    if not check_supabase_connection(supabase_url):
        print(f"ERROR: Cannot connect to Supabase URL: {supabase_url}")
        print("Please check:")
        print("1. Your SUPABASE_URL is correct")
        print("2. Your network/VPN settings aren't blocking the connection")
        print("3. No proxy settings are interfering with the connection")
        return False
    
    try:
        # Initialize Supabase client
        print("Initializing Supabase client...")
        supabase = create_client(supabase_url, supabase_key)
        
        # Format data for Supabase
        formatted_data = [format_product_for_supabase(product, brand_id) for product in data]
        
        # Insert data into product table
        print(f"Uploading {len(formatted_data)} products to Supabase...")
        
        # Use upsert to handle duplicate SKUs gracefully
        result = supabase.table('product').upsert(formatted_data).execute()
        
        print(f"Successfully uploaded {len(formatted_data)} products to Supabase")
        return True
        
    except Exception as e:
        print(f"ERROR: Failed to upload data to Supabase: {str(e)}")
        import traceback
        traceback.print_exc()
        
        # Show more helpful troubleshooting message
        print("\nTROUBLESHOOTING SUGGESTIONS:")
        print("1. Check if your SUPABASE_URL and SUPABASE_KEY are correct")
        print("2. Verify your internet connection")
        print("3. Check if you can access the Supabase dashboard in your browser")
        print("4. If using a VPN, try disabling it temporarily")
        print("5. Make sure the 'product' table exists in your Supabase project")
        
        return False

def main():
    """Main entry point."""
    # Check command line arguments
    if len(sys.argv) < 2:
        print("Usage: python upload_to_supabase.py <data_file.json> [brand_id]")
        print("\nExample:")
        print("  python upload_to_supabase.py data/khaadi_20250420_203618.json 123e4567-e89b-12d3-a456-426614174000")
        return 1
    
    # Get data file path
    data_file = sys.argv[1]
    
    # Get optional brand_id
    brand_id = sys.argv[2] if len(sys.argv) > 2 else None
    
    # Load data
    print(f"Loading data from {data_file}...")
    data = load_scraped_data(data_file)
    
    if not data:
        print("No data found to upload")
        return 1
    
    print(f"Found {len(data)} products to upload")
    
    # Upload to Supabase
    success = upload_to_supabase(data, brand_id)
    
    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main()) 