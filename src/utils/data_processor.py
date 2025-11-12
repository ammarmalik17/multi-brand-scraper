import json
import os
import csv
import pandas as pd
from datetime import datetime
from typing import List, Dict, Any, Optional

from .database_manager import DatabaseManager

class DataProcessor:
    """Utility class to process and save scraped data."""
    
    @staticmethod
    def save_to_json(data: List[Dict[str, Any]], brand_name: str, output_dir: str = 'data') -> str:
        """
        Save scraped data to a JSON file.
        
        Args:
            data: List of dictionaries containing scraped data
            brand_name: Name of the brand
            output_dir: Directory to save the file in
            
        Returns:
            Path to the saved file
        """
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"{brand_name.lower().replace(' ', '_')}_{timestamp}.json"
        filepath = os.path.join(output_dir, filename)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        
        return filepath
    
    @staticmethod
    def save_to_csv(data: List[Dict[str, Any]], brand_name: str, output_dir: str = 'data') -> str:
        """
        Save scraped data to a CSV file.
        
        Args:
            data: List of dictionaries containing scraped data
            brand_name: Name of the brand
            output_dir: Directory to save the file in
            
        Returns:
            Path to the saved file
        """
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"{brand_name.lower().replace(' ', '_')}_{timestamp}.csv"
        filepath = os.path.join(output_dir, filename)
        
        if not data:
            return ""
        
        # Get all unique keys from all dictionaries
        fieldnames = set()
        for item in data:
            fieldnames.update(item.keys())
        
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=sorted(fieldnames))
            writer.writeheader()
            writer.writerows(data)
        
        return filepath
    
    @staticmethod
    def to_dataframe(data: List[Dict[str, Any]]) -> pd.DataFrame:
        """
        Convert scraped data to a pandas DataFrame.
        
        Args:
            data: List of dictionaries containing scraped data
            
        Returns:
            pandas DataFrame
        """
        return pd.DataFrame(data)
    
    @staticmethod
    def filter_data(data: List[Dict[str, Any]], filter_func: callable) -> List[Dict[str, Any]]:
        """
        Filter scraped data based on a filter function.
        
        Args:
            data: List of dictionaries containing scraped data
            filter_func: Function that takes a dict and returns a boolean
            
        Returns:
            Filtered list of dictionaries
        """
        return list(filter(filter_func, data))
    
    @staticmethod
    def merge_data(data_sets: List[List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
        """
        Merge multiple data sets into one.
        
        Args:
            data_sets: List of data sets to merge
            
        Returns:
            Merged list of dictionaries
        """
        merged_data = []
        for data_set in data_sets:
            merged_data.extend(data_set)
        
        return merged_data 