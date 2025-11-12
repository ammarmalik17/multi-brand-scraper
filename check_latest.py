import os
import sys
from dotenv import load_dotenv
from supabase import create_client, Client

# Utility script to check the latest added data for a specific brand
# Run this script with: python check_latest.py <brand_name>

def main():
    if len(sys.argv) < 2:
        print("Usage: python check_latest.py <brand_name>")
        return 1
        
    brand_name = sys.argv[1]
    
    # Load environment variables
    load_dotenv()
    
    # Get Supabase credentials
    supabase_url = os.environ.get("SUPABASE_URL")
    supabase_key = os.environ.get("SUPABASE_KEY")
    
    if not supabase_url or not supabase_key:
        print("Error: Missing Supabase credentials in .env file")
        return 1
    
    try:
        # Initialize Supabase client
        print(f"Initializing Supabase client for brand: {brand_name}...")
        supabase = create_client(supabase_url, supabase_key)
        
        # Fetch data from the table with fields in standardized order
        print(f"Fetching data from 'product_listing' table for brand: {brand_name}...")
        try:
            # Try with all fields including newer ones
            response = supabase.table("product_listing").select(
                "id, brand, sku, name, product_type, category, " + 
                "fabric_slug, fabric_style, fabric_type, color, " +
                "price, original_price, discount_percentage, status, created_at"
            ).eq("brand", brand_name).order("created_at", {"ascending": False}).limit(10).execute()
            
            if not response.data:
                print(f"No data found for brand: {brand_name}")
                return 0
            
            # Print the data in a structured format
            print(f"\nFound {len(response.data)} {brand_name} records:")
            for i, item in enumerate(response.data, 1):
                # Header with main identification
                print(f"{i}. [{item['brand']}] {item['name']} (SKU: {item['sku']})")
                
                # Classification and fabric details
                classification = f"{item.get('product_type', 'N/A')} - {item.get('category', 'N/A')}"
                
                fabric_info = ""
                if item.get('fabric_slug'):
                    fabric_info += f" | Fabric: {item.get('fabric_slug', '')}"
                elif item.get('fabric_style') or item.get('fabric_type'):
                    fabric_info = f" | {item.get('fabric_style', '')} {item.get('fabric_type', '')}"
                
                if item.get('color'):
                    fabric_info += f" | Color: {item.get('color', '')}"
                
                print(f"   Type: {classification}{fabric_info}")
                
                # Price information
                discount_info = ""
                if item.get('discount_percentage'):
                    discount_info = f" (Discount: {item.get('discount_percentage')}%)"
                
                original = f" | Original: {item.get('original_price', 'N/A')}" if item.get('original_price') else ""
                print(f"   Price: {item.get('price', 'N/A')}{original}{discount_info}")
                
                # Status and metadata
                print(f"   Status: {item.get('status', 'active')} | Created: {item['created_at']}")
                print()
                
            return 0
            
        except Exception as e:
            print(f"Error: {str(e)}")
            return 1
            
    except Exception as e:
        print(f"Error: {str(e)}")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 