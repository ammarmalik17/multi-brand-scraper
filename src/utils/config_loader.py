import json
import os
from typing import Dict, Any, List

class ConfigLoader:
    """Utility class to load scraper configurations."""
    
    @staticmethod
    def load_config(brand_name: str) -> Dict[str, Any]:
        """
        Load configuration for a specific brand.
        
        Args:
            brand_name: Name of the brand to load configuration for
            
        Returns:
            Dictionary containing brand configuration
            
        Raises:
            FileNotFoundError: If the configuration file does not exist
            json.JSONDecodeError: If the configuration file is not valid JSON
        """
        # First try the new brand directory structure
        brand_config_path = os.path.join('brands', brand_name.lower().replace(' ', '_'), 'config', 'config.json')
        
        if os.path.exists(brand_config_path):
            with open(brand_config_path, 'r') as f:
                config = json.load(f)
            return config
        
        # Fallback to the old structure
        old_config_path = os.path.join('config', f"{brand_name.lower().replace(' ', '_')}_config.json")
        
        if os.path.exists(old_config_path):
            with open(old_config_path, 'r') as f:
                config = json.load(f)
            return config
        
        raise FileNotFoundError(f"Configuration file for {brand_name} not found at either {brand_config_path} or {old_config_path}")
    
    @staticmethod
    def load_all_configs() -> List[Dict[str, Any]]:
        """
        Load configurations for all brands.
        
        Returns:
            List of dictionaries containing brand configurations
        """
        configs = []
        
        # First try to load from the new brand directory structure
        brands_dir = 'brands'
        if os.path.exists(brands_dir):
            for brand_dir in os.listdir(brands_dir):
                brand_config_path = os.path.join(brands_dir, brand_dir, 'config', 'config.json')
                if os.path.exists(brand_config_path):
                    try:
                        with open(brand_config_path, 'r') as f:
                            config = json.load(f)
                        configs.append(config)
                    except (json.JSONDecodeError, IOError) as e:
                        print(f"Error loading configuration from {brand_config_path}: {str(e)}")
        
        # Then try the old config directory for any remaining configs
        config_dir = 'config'
        if os.path.exists(config_dir):
            for filename in os.listdir(config_dir):
                if filename.endswith('_config.json'):
                    try:
                        config_path = os.path.join(config_dir, filename)
                        with open(config_path, 'r') as f:
                            config = json.load(f)
                        configs.append(config)
                    except (json.JSONDecodeError, IOError) as e:
                        print(f"Error loading configuration from {filename}: {str(e)}")
        
        return configs
    
    @staticmethod
    def get_available_brands() -> List[str]:
        """
        Get list of available brand names from configuration files.
        
        Returns:
            List of brand names
        """
        brands = set()
        
        # First check the new brand directory structure
        brands_dir = 'brands'
        if os.path.exists(brands_dir):
            for brand_dir in os.listdir(brands_dir):
                brand_config_path = os.path.join(brands_dir, brand_dir, 'config', 'config.json')
                if os.path.exists(brand_config_path):
                    brand_name = brand_dir.replace('_', ' ').title()
                    brands.add(brand_name)
        
        # Then check the old config directory
        config_dir = 'config'
        if os.path.exists(config_dir):
            for filename in os.listdir(config_dir):
                if filename.endswith('_config.json'):
                    brand_name = filename.replace('_config.json', '').replace('_', ' ').title()
                    brands.add(brand_name)
        
        return list(brands) 