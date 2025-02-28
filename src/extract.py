import random
import asyncio
import time
from datetime import datetime, timedelta
from typing import List

import pandas as pd
import aiohttp
from aiolimiter import AsyncLimiter
from tqdm import tqdm

from config.config import config
from .utils.helper import process_document_response
from .utils.logger import logger


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
        df.reset_index(drop=True, inplace=True)

        tqdm.write(
            f'Filtered to {len(df)} relevant companies. (exclusions: unlisted companies, NonConsolidated, null names)'
        )
        return df
    except Exception as e:
        tqdm.write(f'Failed to extract company information: {e}')
        raise


async def get_documents_by_date(date_str: str, session: aiohttp.ClientSession, limiter: AsyncLimiter) -> dict:
    """Asynchronously get documents for a specific date.

    Args:
        date_str (str): Date in format 'YYYY-MM-DD'

    Returns:
        dict: Documents for the date
    """
    url = f'{config.base_url}/documents.json'
    params = {'date': date_str, 'type': '2', 'Subscription-Key': config.api_key}

    async with limiter:
        async with session.get(url, params=params) as response:
            response.raise_for_status()
            return await response.json()


async def fetch_data_with_retry(date_str: str, session: aiohttp.ClientSession, limiter: AsyncLimiter) -> list:
    """Asynchronously fetch documents for a date with retry logic.
    """
    for attempt in range(config.max_retries + 1):
        try:
            response_data = await get_documents_by_date(date_str, session, limiter)
            if response_data and 'results' in response_data:
                return response_data['results']
            return []
        except Exception as e:
            if attempt < config.max_retries:
                retry_wait = config.retry_delay * (2 ** attempt) * (0.1 + random.random())
                logger.warning(f'retry {attempt+1}/{config.max_retries} for {date_str} after {retry_wait:.2f}s')
                await asyncio.sleep(retry_wait)
            else:
                logger.error(f'failed to fetch documents for {date_str} after {config.max_retries} retries: {e}')
                return []


async def get_documents_by_date_range(
        start_date: str,
        end_date: str,
        session: aiohttp.ClientSession,
        limiter: AsyncLimiter
) -> List[dict]:
    """
    Asynchronously get documents for a date range.

    Args:
        start_date (str): Start date in format 'YYYY-MM-DD'
        end_date (str): End date in format 'YYYY-MM-DD'

    Returns:
        List[dict]: List of documents for the date range
    """
    date_format = '%Y-%m-%d'
    start_date_obj = datetime.strptime(start_date, date_format)
    end_date_obj = datetime.strptime(end_date, date_format)
    if start_date_obj > end_date_obj:
        raise ValueError("start_date must be <= end_date")

    dates = []
    current_date = start_date_obj
    while current_date <= end_date_obj:
        dates.append(current_date.strftime('%Y-%m-%d'))
        current_date += timedelta(days=1)

    documents = []
    progress_bar = tqdm(
        total=len(dates),
        desc='Fetching list of documents from Get List API endpoint',
        unit='date',
    )
    tasks = [asyncio.create_task(fetch_data_with_retry(date_str, session, limiter)) for date_str in dates]

    # as each task completes, update progresses and extend the results
    for task in asyncio.as_completed(tasks):
        res = await task
        documents.extend(res)
        progress_bar.update(1)

    progress_bar.close()
    return documents


def filter_documents(documents: list, doc_types: List) -> list:
    """Filter the list of documents based on EDINET code and document types.

    Args:
        documents (list): List of documents to filter
        doc_types (List): List of document types to keep

    Returns:
        list: Filtered list of documents
    """
    companies = extract_companies()
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


async def get_document_by_id(doc_info: dict, session: aiohttp.ClientSession, limiter: AsyncLimiter) -> str:
    """Asynchronously retrieve and process a document based on its information.

    Args:
        doc_info (dict): Document information containing docID and flags
        session (aiohttp.ClientSession): Session to use for the API call
        limiter (AsyncLimiter): Limiter to use for the API call

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

    async with limiter:
        async with session.get(url, params=params) as response:
            response.raise_for_status()
            content = await response.read()
            return process_document_response(content, doc_info)


async def download_document_with_retry(
        doc: dict,
        session: aiohttp.ClientSession,
        limiter: AsyncLimiter
) -> str:
    """
    Asynchronously download a single document with retry logic.

    Args:
        doc (dict): Document information containing docID and flags
        session (aiohttp.ClientSession): Session to use for the API call
        limiter (AsyncLimiter): Limiter to use for the API call

    Returns:
        str: Path to the processed file, or None if processing failed
    """
    for attempt in range(config.max_retries + 1):
        try:
            result = await get_document_by_id(doc, session, limiter)
            return result
        except aiohttp.ClientResponseError as e:
            if e.status == 429:
                # this error is for hitting rate limit, we should respect the retry-after header if present
                retry_after = int(e.headers.get('Retry-After', 10))
                tqdm.write(f"Received 429 for document {doc['docID']}. Retrying after {retry_after} seconds.")
                await asyncio.sleep(retry_after)
            elif attempt < config.max_retries:
                retry_wait = config.retry_delay * (2 ** attempt) * (0.5 + random.random())
                tqdm.write(f"Retrying download for {doc['docID']} (attempt {attempt + 2}) after {retry_wait:.2f}s due to: {e}")
                await asyncio.sleep(retry_wait)
            else:
                tqdm.write(f"Failed to download document {doc['docID']} after {config.max_retries} retries: {e}")
                return None
        except Exception as e:
            if attempt < config.max_retries:
                retry_wait = config.retry_delay * (2 ** attempt) * (0.5 + random.random())
                tqdm.write(f"retrying download for {doc['docID']} (attempt {attempt + 2}) after {retry_wait:.2f}s")
                await asyncio.sleep(retry_wait)
            else:
                tqdm.write(f"Failed to download document {doc['docID']} after {config.max_retries} retries: {e}")
                return None


async def download_documents(
        doc_list: list,
        session: aiohttp.ClientSession,
        limiter: AsyncLimiter,
        companies_to_get: int = None
) -> list:
    """
    Asynchronously download multiple documents concurrently.

    Args:
        doc_list (list): List of document IDs to download.
        companies_to_get (int): Number of companies to download. If None, all documents will be downloaded.
        session (aiohttp.ClientSession): Session to use for the API call
        limiter (AsyncLimiter): Limiter to use for the API call

    Returns:
        list: List of paths to the downloaded files.
    """
    if companies_to_get is not None:
        doc_list = doc_list[:companies_to_get]

    downloaded_files = []

    progress_bar = tqdm(total=len(doc_list), desc='Downloading documents', unit='doc')

    async def task_warapper(doc):
        result = await download_document_with_retry(doc, session, limiter)
        progress_bar.update(1)
        return result
    
    tasks = [task_warapper(doc) for doc in doc_list]
    results = await asyncio.gather(*tasks)
    progress_bar.close()
    downloaded_files = [r for r in results if r is not None]
    return downloaded_files

