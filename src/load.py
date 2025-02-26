import pandas as pd
import os
from .utils.logger import logger

def load_to_csv(df: pd.DataFrame, output_path: str) -> None:
    """
    Writes the given DataFrame to a CSV file at the specified path.

    Args:
        df: The final, transformed pandas DataFrame containing the financial data
        output_path: The path to the CSV file to be written
    """
    if df.empty:
        logger.warning("Attempted to load an empty DataFrame to CSV. Aborting.")
        return
    
    
    try:
        # Create output directory if it doesn't exist
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        df.to_csv(output_path, index=False, encoding='utf-8')
        logger.info(f"Data successfully loaded to CSV: {output_path}")
    except Exception as e:
        logger.error(f"Error while writing CSV to {output_path}: {e}")
