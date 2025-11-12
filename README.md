# Multi-Brand Web Scraper

A flexible and extensible web scraper that can handle multiple brands with separate configurations, with Supabase integration for data storage.

## Features

- **Modular Architecture**: Separate scraper implementation for each brand with shared base functionality
- **Configuration-Driven**: Individual JSON configuration files for each brand enable easy customization
- **Multiple Scraping Approaches**: Support for both HTML parsing and API-based scraping
- **Flexible Data Export**: Data export in multiple formats (JSON, CSV) with pandas DataFrame support
- **Supabase Integration**: Complete integration with Supabase for centralized data storage
- **Anti-Blocking Measures**: User agent rotation, referrer spoofing, and exponential backoff retry logic
- **Parallel Processing**: Concurrent scraping using ThreadPoolExecutor for improved performance
- **Command-Line Interface**: Intuitive CLI with multiple options for flexible usage
- **Error Handling & Logging**: Comprehensive exception handling and structured logging
- **Extensibility**: Easy to add new brands by following the established directory structure

## Supported Brands

1. **Sapphire Online** - Pagination-based HTML scraping with AJAX requests
2. **Khaadi** - Pagination-based HTML scraping with AJAX requests

Both scrapers implement advanced features including:
- Category-based scraping with configurable limits
- Product detail enhancement with additional API calls
- Anti-blocking measures to avoid detection
- Parallel processing for improved performance

## Project Structure

```
.
├── brands/                        # Brand-specific packages
│   ├── __init__.py                # Exports brand scrapers
│   ├── base_scraper.py            # Base scraper class for all brands
│   ├── sapphire/                  # Sapphire brand package
│   │   ├── __init__.py            # Exports Sapphire scraper
│   │   ├── config/                # Brand-specific configuration
│   │   │   └── config.json        # Brand configuration
│   │   └── scraper/               # Brand-specific scraper
│   │       └── scraper.py         # Scraper implementation
│   └── khaadi/                    # Khaadi brand package
│       ├── __init__.py            # Exports Khaadi scraper
│       ├── config/                # Brand-specific configuration
│       │   └── config.json        # Brand configuration
│       └── scraper/               # Brand-specific scraper
│           └── scraper.py         # Scraper implementation
├── src/                           # Source code
│   ├── __init__.py                # Package initializer
│   ├── scraper_factory.py         # Factory to create brand-specific scrapers
│   └── utils/                     # Utility modules
│       ├── __init__.py            # Package initializer
│       ├── config_loader.py       # Utility to load brand configurations
│       ├── data_processor.py      # Utility to process and save data
│       ├── database_manager.py    # Manager for database operations
│       └── request_utils.py       # Anti-blocking utilities for HTTP requests
├── data/                          # Output directory for scraped data
├── main.py                        # Main entry point
├── check_latest.py                # Check latest scraped data
├── check_supabase.py              # Verify connection to Supabase and view data
├── setup_supabase.py              # Sets up Supabase table structure
├── update_supabase_schema.py      # Update Supabase schema
├── upload_to_supabase.py          # Utility to upload data to Supabase
├── .env                           # Environment variables for Supabase (not in repo)
└── requirements.txt               # Project dependencies
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

5. Verify your setup:
   - Test Supabase connectivity: `python check_supabase.py`
   - Check latest scraped data: `python check_latest.py`

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

The project includes comprehensive integration with Supabase for storing scraped data:

1. **Product Table Schema**: Created by `setup_supabase.py`, includes fields for complete product details.
2. **Brand Table**: References brands with their IDs for proper data organization.
3. **Automatic Data Upload**: Products are automatically formatted and uploaded to Supabase when using the `--supabase` flag in main.py.
4. **Manual Upload Utility**: The `upload_to_supabase.py` script allows uploading existing JSON files to Supabase.
5. **Data Validation**: The upload process includes validation and error handling.
6. **Connection Verification**: The `check_supabase.py` script verifies connectivity and displays sample data.
7. **Schema Updates**: The `update_supabase_schema.py` script handles schema modifications.

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

### Configuration Options

Brand configurations support numerous options including:
- Base URL and API endpoints
- HTTP headers and timeout settings
- Category definitions for scraping
- CSS selectors for data extraction
- Rate limiting and parallel processing parameters
- Product enhancement settings

Refer to existing brand configurations for examples of available options.

## License

MIT 