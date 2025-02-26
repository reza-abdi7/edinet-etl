import pandas as pd
import os
from .utils.logger import logger

def load_to_csv(df: pd.DataFrame, output_path: str, file_name: str) -> None:
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
        full_file_path = os.path.join(output_path, file_name)
        os.makedirs(os.path.dirname(full_file_path), exist_ok=True)
        
        df.to_csv(full_file_path, index=False, encoding='utf-8')
        logger.info(f"Data successfully loaded to CSV: {output_path}")
    except Exception as e:
        logger.error(f"Error while writing CSV to {output_path}: {e}")
