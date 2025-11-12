# Multi-Brand Web Scraper

A flexible and extensible web scraper that can handle multiple brands with separate configurations, with Supabase integration for data storage.

## Features

- Modular architecture with separate scraper for each brand
- Individual configuration files for each brand
- Support for HTML and API-based scraping
- Data export in multiple formats (JSON, CSV)
- Supabase integration for centralized data storage
- Command-line interface for easy usage

## Supported Brands

1. Sapphire Online - Pagination-based HTML scraping with AJAX requests
2. Khaadi - Pagination-based HTML scraping with AJAX requests

## Project Structure

```
.
├── brands/                     # Brand-specific packages
│   ├── base_scraper.py         # Base scraper class for all brands
│   ├── sapphire/               # Sapphire brand package
│   │   ├── config/             # Brand-specific configuration
│   │   │   └── config.json     # Brand configuration
│   │   └── scraper/            # Brand-specific scraper
│   │       └── scraper.py      # Scraper implementation
│   └── khaadi/                 # Khaadi brand package
│       ├── config/             # Brand-specific configuration
│       │   └── config.json     # Brand configuration
│       └── scraper/            # Brand-specific scraper
│           └── scraper.py      # Scraper implementation
├── config/                     # Legacy configuration files
├── src/                        # Source code
│   ├── scrapers/               # Legacy scrapers directory
│   ├── utils/                  # Utility modules
│   │   ├── config_loader.py    # Utility to load brand configurations
│   │   ├── data_processor.py   # Utility to process and save data
│   │   └── database_manager.py # Manager for database operations
│   └── scraper_factory.py      # Factory to create brand-specific scrapers
├── data/                       # Output directory for scraped data
├── main.py                     # Main entry point
├── upload_to_supabase.py       # Utility to upload data to Supabase
├── setup_supabase.py           # Sets up Supabase table structure
├── check_supabase.py           # Verify connection to Supabase
├── update_supabase_schema.py   # Update Supabase schema
├── .env                        # Environment variables for Supabase (not in repo)
└── requirements.txt            # Project dependencies
```

## Installation

1. Clone the repository:
   ```
   git clone https://github.com/yourusername/multi-brand-scraper.git
   cd multi-brand-scraper
   ```

2. Create and activate a virtual environment:
   ```
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. Install dependencies:
   ```
   pip install -r requirements.txt
   ```

4. Set up Supabase integration (if needed):
   - Create a `.env` file with your Supabase credentials:
     ```
     SUPABASE_URL=your_project_url
     SUPABASE_KEY=your_api_key
     ```
   - For direct PostgreSQL access (used by setup script):
     ```
     host=your-supabase-db-host
     port=your-supabase-db-port
     dbname=your-supabase-db-name
     user=your-supabase-db-user
     password=your-supabase-db-password
     ```
   - Run the setup script to create the required tables:
     ```
     python setup_supabase.py
     ```

## Usage

### List Available Brands

```
python main.py --list
```

### Scrape a Specific Brand

For Sapphire Online:

```
python main.py --brand sapphire
```

For Khaadi:

```
python main.py --brand khaadi
```

### Specify Output Format

```
python main.py --brand sapphire --format csv
```

### Save to Supabase

```
python main.py --brand sapphire --supabase
```

### Scrape All Brands and Save to Supabase

```
python main.py --all --supabase
```

### Upload Existing Data to Supabase

```
python upload_to_supabase.py --file data/brand_name_20230701_120000.json --brand-id your-brand-uuid
```

## Supabase Integration

The project includes integration with Supabase for storing scraped data:

1. **Product Table Schema**: Created by `setup_supabase.py`, includes fields for product details.
2. **Brand Table**: References brands with their IDs.
3. **Data Upload**: Products are automatically formatted and uploaded to Supabase when using the `--supabase` flag.
4. **Data Validation**: The upload process includes validation and error handling.

## Troubleshooting Supabase Connection

If you encounter issues with Supabase:

1. Verify your `.env` file contains correct credentials
2. Check your internet connection
3. Run `python check_supabase.py` to test connectivity
4. Ensure your VPN or firewall isn't blocking the connection

## Adding a New Brand

1. Create a new brand directory in the `brands` directory
2. Add the following subdirectories:
   - `config` - for brand configuration
   - `scraper` - for brand scraper implementation
3. Create a scraper implementation in `brands/your_brand/scraper/scraper.py` that inherits from `BaseScraper`
4. Create a configuration file in `brands/your_brand/config/config.json`
5. Add an `__init__.py` file in your brand directory that exposes your scraper class
6. Update the main brands `__init__.py` file to include your new scraper
7. Add the brand to Supabase brand table if using Supabase integration

## License

MIT 