from typing import Dict, Any, Optional

from brands import BaseScraper, SapphireScraper, KhaadiScraper
from src.utils.config_loader import ConfigLoader

class ScraperFactory:
    """Factory class to create scrapers for different brands."""
    
    @staticmethod
    def create_scraper(brand_name: str) -> Optional[BaseScraper]:
        """
        Create a scraper for the specified brand.
        
        Args:
            brand_name: Name of the brand to create a scraper for
            
        Returns:
            Scraper instance for the specified brand or None if not supported
        """
        try:
            # Load configuration for the brand
            config = ConfigLoader.load_config(brand_name)
            
            # Create scraper based on brand name
            if brand_name.lower() == 'sapphire':
                return SapphireScraper(config)
            elif brand_name.lower() == 'khaadi':
                return KhaadiScraper(config)
            else:
                print(f"Scraper for {brand_name} not implemented")
                return None
                
        except (FileNotFoundError, Exception) as e:
            print(f"Error creating scraper for {brand_name}: {str(e)}")
            return None
    
    @staticmethod
    def get_available_scrapers() -> Dict[str, str]:
        """
        Get mapping of available brand names to scraper class names.
        
        Returns:
            Dictionary mapping brand names to scraper class names
        """
        return {
            'sapphire': 'SapphireScraper',
            'khaadi': 'KhaadiScraper'
        } 