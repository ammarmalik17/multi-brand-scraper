import os
import sys
import psycopg2
from dotenv import load_dotenv

# This script updates the existing product_listing table with new columns
# Run this if you've already created the table but need to add new fields

def main():
    # Load environment variables
    load_dotenv()
    
    # Get Supabase PostgreSQL connection details
    db_host = os.environ.get("host")
    db_port = os.environ.get("port")
    db_name = os.environ.get("dbname")
    db_user = os.environ.get("user")
    db_password = os.environ.get("password")
    
    # SQL to update the table with new columns
    update_table_sql = """
    -- Add fabric detail columns if they don't exist
    DO $$ 
    BEGIN 
        BEGIN
            ALTER TABLE public.product_listing ADD COLUMN fabric_slug TEXT;
        EXCEPTION
            WHEN duplicate_column THEN RAISE NOTICE 'column fabric_slug already exists';
        END;
        
        BEGIN
            ALTER TABLE public.product_listing ADD COLUMN fabric_style TEXT;
        EXCEPTION
            WHEN duplicate_column THEN RAISE NOTICE 'column fabric_style already exists';
        END;
        
        BEGIN
            ALTER TABLE public.product_listing ADD COLUMN fabric_type TEXT;
        EXCEPTION
            WHEN duplicate_column THEN RAISE NOTICE 'column fabric_type already exists';
        END;
        
        BEGIN
            ALTER TABLE public.product_listing ADD COLUMN color TEXT;
        EXCEPTION
            WHEN duplicate_column THEN RAISE NOTICE 'column color already exists';
        END;
        
        -- Add image columns if they don't exist
        BEGIN
            ALTER TABLE public.product_listing ADD COLUMN primary_image_url TEXT;
        EXCEPTION
            WHEN duplicate_column THEN RAISE NOTICE 'column primary_image_url already exists';
        END;
        
        BEGIN
            ALTER TABLE public.product_listing ADD COLUMN hover_image_url TEXT;
        EXCEPTION
            WHEN duplicate_column THEN RAISE NOTICE 'column hover_image_url already exists';
        END;
        
        -- Add pricing columns if they don't exist
        BEGIN
            ALTER TABLE public.product_listing ADD COLUMN discount_percentage NUMERIC;
        EXCEPTION
            WHEN duplicate_column THEN RAISE NOTICE 'column discount_percentage already exists';
        END;
    END $$;
    
    -- Copy data from old columns to new ones if needed
    UPDATE public.product_listing 
    SET primary_image_url = image_url 
    WHERE primary_image_url IS NULL AND image_url IS NOT NULL;
    """
    
    print("Updating Supabase table schema...")
    
    # Test if we have all the connection details
    if not all([db_host, db_port, db_name, db_user, db_password]):
        print("Error: Missing database connection details in .env file")
        return 1
    
    try:
        # Connect to the PostgreSQL database
        print(f"Connecting to PostgreSQL database at {db_host}:{db_port}...")
        conn = psycopg2.connect(
            host=db_host,
            port=db_port,
            dbname=db_name,
            user=db_user,
            password=db_password
        )
        
        # Create a cursor
        cur = conn.cursor()
        
        # Execute the SQL to update the table
        print("Executing schema update...")
        cur.execute(update_table_sql)
        
        # Commit the transaction
        conn.commit()
        
        print("Schema updated successfully!")
        
        # Close the cursor and connection
        cur.close()
        conn.close()
        
        print("\nUpdate completed successfully!")
        return 0
        
    except Exception as e:
        print(f"Error: {str(e)}")
        return 1

if __name__ == "__main__":
    sys.exit(main()) 