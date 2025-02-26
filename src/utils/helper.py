import os
import zipfile
import shutil
import traceback
from pathlib import Path
from functools import wraps
from .logger import logger
from config.config import config


def cleanup_temp_dir(doc_id):
    """
    Cleanup temporary directory for a document if it exists.
    
    Args:
        doc_id: Document ID string
        
    Returns:
        bool: True if cleanup was successful or directory didn't exist, False if an error occurred
    """
    temp_dir = Path(config.output_dir) / "temp" / doc_id
    if not temp_dir.exists():
        return True
        
    try:
        shutil.rmtree(temp_dir)
        logger.debug(f"Cleaned up temporary directory for document {doc_id}")
        return True
    except Exception as e:
        logger.error(f"Failed to clean up temporary directory for document {doc_id}: {str(e)}")
        return False


def save_and_extract_document(content: bytes, doc_info: dict, file_type: str) -> str:
    """
    Save zip content and extract the required file. due to downloading many files and since we only need
    csv or xbrl file, we first save zip file and then unzip it in a temp dir. then we will be looking for
    csv or xbrl extension with priority for csv.
    we only keep this file and remove the rest.
    this will be done for each document retrieved.
    
    Args:
        content: Binary content of the zip file
        doc_info: Document information dictionary
        file_type: Type of file to extract ('csv' or 'xbrl')
    
    Returns:
        str: Path to the extracted file, or None if extraction failed
    """
    # Create temp directory for extraction
    temp_dir = Path(config.output_dir) / "temp" / doc_info['docID']
    temp_dir.mkdir(parents=True, exist_ok=True)
    
    
    zip_path = temp_dir / f"{doc_info['docID']}.zip"
    try:
        # 1.Save zip file
        with open(zip_path, 'wb') as f:
            f.write(content)
    
        # 2.Extract zip
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(temp_dir)
    except Exception as e:
        logger.error(f"Error processing zip for docment {doc_info['docID']}: {e}")
        cleanup_temp_dir(doc_info['docID'])
        return None
    
    # 3.Find the target file
    extension = '.csv' if file_type == 'csv' else '.xbrl'
    target_files = list(temp_dir.rglob(f"*{extension}"))
    if not target_files:
        logger.warning(f"No {extension} files found in the zip for {doc_info['docID']}")
        return None
    
    # 4.Get the largest file if multiple files exist
    target_file = max(target_files, key=lambda x: x.stat().st_size)
    
    # 5.Ensure output directory exists and create final filename
    output_dir = Path(config.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    date_str = doc_info['submitDateTime'].split()[0].replace('-', '')
    final_filename = f"{doc_info['edinetCode']}_{date_str}_{doc_info['docTypeCode']}{extension}"
    final_path = output_dir / final_filename
    
    try:
        # 6.Copy the file to final location
        shutil.copy2(target_file, final_path)
    except Exception as e:
        logger.error(f"Error saving extracted file for document {doc_info['docID']}: {e}")
        return None
    
    # 7.Clean up temp directory
    cleanup_temp_dir(doc_info['docID'])
    
    logger.info(f"Successfully extracted and saved {final_path}")
    return str(final_path)


def process_document_response(content: bytes, doc_info: dict) -> str:
    """
    Process the document response based on available flags.
    
    Args:
        content: Binary content of the zip file
        doc_info: Document information dictionary
    
    Returns:
        str: Path to the processed file, or None if processing failed
    """
    if doc_info.get('csvFlag') == '1':
        return save_and_extract_document(content, doc_info, 'csv')
    elif doc_info.get('xbrlFlag') == '1':
        return save_and_extract_document(content, doc_info, 'xbrl')
    else:
        logger.warning(f"No supported format available for {doc_info['docID']}")
        return None
