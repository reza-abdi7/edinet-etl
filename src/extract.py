import random
import time
from datetime import datetime, timedelta
from typing import List

import pandas as pd
from requests import Session
from requests_ratelimiter import LimiterAdapter
from tqdm import tqdm

from config.config import config

from .utils.helper import process_document_response
from .utils.logger import logger

# Create a global session with rate limiting applied.
session = Session()
adapter = LimiterAdapter(per_second=config.request_per_second)
session.mount('http://', adapter)
session.mount('https://', adapter)


def extract_companies(csv_file: str = config.csv_file) -> pd.DataFrame:
    """
    Extract company information from the CSV file.
    Note that the file is downloaded from EDINET website.
    Args:
        csv_file (str): Path to the CSV file.
    Returns:
        pd.DataFrame: DataFrame containing the extracted company information.
    """
    try:
        # Read the CSV file
        df = pd.read_csv(csv_file, encoding='cp932')
        tqdm.write(f'Extracted {len(df)} companies from {csv_file}')

        columns_to_keep = [
            'EDINET Code',
            'Listed company / Unlisted company',
            'Consolidated / NonConsolidated',
            'account closing date',
            'Submitter Name（alphabetic）',
            "Submitter's industry",
        ]
        df = df[columns_to_keep]

        # Filter for listed companies only, consolidated only and remove companies with null names
        df = df[df['Listed company / Unlisted company'] == 'Listed company']
        df = df[df['Consolidated / NonConsolidated'] == 'Consolidated']
        df = df.dropna(subset=['Submitter Name（alphabetic）'])

        # Exclude specified industries
        # industry_exclude = [
        #     'Services',
        #     'Real Estate',
        #     'Securities & Commodity Futures',
        #     'Banks',
        #     'Insurance',
        # ]
        # df = df[~df["Submitter's industry"].isin(industry_exclude)]
        df.reset_index(drop=True, inplace=True)

        tqdm.write(
            f'Filtered to {len(df)} relevant companies. (exclusions: unlisted companies, NonConsolidated, null names)'
        )
        return df
    except Exception as e:
        tqdm.write(f'Failed to extract company information: {e}')
        raise


def get_documents_by_date(date_str: str) -> dict:
    """Get documents for a specific date using a sequential request with rate limiting.

    Args:
        date_str (str): Date in format 'YYYY-MM-DD'

    Returns:
        dict: Documents for the date
    """
    url = f'{config.base_url}/documents.json'
    params = {'date': date_str, 'type': '2', 'Subscription-Key': config.api_key}

    # Using the session with the rate limiting adapter
    response = session.get(url, params=params)
    response.raise_for_status()
    return response.json()


def get_documents_by_date_range(start_date: str, end_date: str) -> List[dict]:
    """
    Get documents for a date range using sequential requests with rate limiting.

    Args:
        start_date (str): Start date in format 'YYYY-MM-DD'
        end_date (str): End date in format 'YYYY-MM-DD'

    Returns:
        List[dict]: List of documents for the date range
    """
    date_format = '%Y-%m-%d'
    if isinstance(start_date, str):
        start_date = datetime.strptime(start_date, date_format)
    if isinstance(end_date, str):
        end_date = datetime.strptime(end_date, date_format)

    dates = []
    current_date = start_date
    while current_date <= end_date:
        dates.append(current_date.strftime('%Y-%m-%d'))
        current_date += timedelta(days=1)

    documents = []

    progress_bar = tqdm(
        total=len(dates),
        desc='Fetching list of documents from Get List API endpoint',
        unit='date',
    )

    for date_str in dates:
        for attempt in range(config.max_retries + 1):
            try:
                response_data = get_documents_by_date(date_str)

                if response_data and 'results' in response_data:
                    documents.extend(response_data['results'])
                break

            except Exception as e:
                if attempt < config.max_retries:
                    retry_wait = config.retry_delay * (2**attempt)
                    retry_wait = retry_wait * (0.1 + random.random())
                    logger.warning(
                        f'Retry {attempt+1}/{config.max_retries} for {date_str} after {retry_wait:.2f}s'
                    )
                    time.sleep(retry_wait)
                else:
                    logger.error(
                        f'Failed to fetch documents for {date_str} after {config.max_retries} retries: {e}'
                    )

        progress_bar.update(1)

    progress_bar.close()
    return documents


def filter_documents(documents, doc_types=config.target_doc_types):
    companies = extract_companies(csv_file=config.csv_file)
    edinet_codes = set(companies['EDINET Code'])
    filtered_docs = []

    with tqdm(
        total=len(documents), desc='Filtering documents', unit='doc'
    ) as progress_bar:
        for doc in documents:
            if doc['edinetCode'] in edinet_codes and doc['docTypeCode'] in doc_types:
                filtered_docs.append(doc)
            progress_bar.update(1)

    return filtered_docs


def get_document_by_id(doc_info: dict) -> str:
    """
    Retrieve and process document based on document info dictionary.
    Args:
        doc_info (dict): Document information containing docID and flags
    Returns:
        str: Path to the processed file, or None if processing failed
    """
    doc_id = doc_info['docID']
    url = f'{config.base_url}/documents/{doc_id}'

    if doc_info.get('csvFlag') == '1':
        file_type = '5'  # CSV
        logger.info(f'Retrieving CSV document for {doc_id}')
    elif doc_info.get('xbrlFlag') == '1':
        file_type = '1'  # XBRL
        logger.info(f'Retrieving XBRL document for {doc_id}')
    else:
        logger.warning(f'No supported format (CSV/XBRL) available for {doc_id}')
        return None

    params = {'type': file_type, 'Subscription-Key': config.api_key}

    response = session.get(url, params=params)
    response.raise_for_status()
    return process_document_response(response.content, doc_info)


def download_documents(doc_list: list, companies_to_get: int = None) -> list:
    """
    Download multiple documents sequentially with rate limiting.

    Args:
        doc_list (list): List of document IDs to download.
        companies_to_get (int): Number of companies to download. If None, all documents will be downloaded.

    Returns:
        list: List of paths to the downloaded files.
    """
    if companies_to_get is not None:
        doc_list = doc_list[:companies_to_get]

    downloaded_files = []

    progress_bar = tqdm(total=len(doc_list), desc='Downloading documents', unit='doc')

    for doc in doc_list:
        for attempt in range(config.max_retries + 1):
            try:
                result = get_document_by_id(doc)
                if result is not None:
                    downloaded_files.append(result)
                break

            except Exception as e:
                if attempt < config.max_retries:
                    retry_wait = config.retry_delay * (2**attempt)
                    retry_wait = retry_wait * (0.5 + random.random())
                    tqdm.write(
                        f"Retrying download for {doc['docID']} (attempt {attempt + 2}) after {retry_wait:.2f}s"
                    )
                    time.sleep(retry_wait)
                else:
                    tqdm.write(
                        f"Failed to download document {doc['docID']} after {config.max_retries} retries: {e}"
                    )

        progress_bar.update(1)

    progress_bar.close()
    return downloaded_files
