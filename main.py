from config.config import config
from src.extract import (
    download_documents,
    extract_companies,
    filter_documents,
    get_documents_by_date_range,
)
from src.load import load_to_csv
from src.transform import process_financial_documents_concurrent
from src.utils.logger import logger

import aiohttp
from aiolimiter import AsyncLimiter
import tqdm
import asyncio


async def main():
    async with aiohttp.ClientSession() as session:

        # Initialize async rate limiter: number value of (config.request_per_second) requests per 1 second.
        limiter = AsyncLimiter(config.request_per_second, time_period=1)

        # 1. Load or construct your company_info DataFrame
        company_info = extract_companies()

        # 2. get list of documents for date range from List Retrieval API endpoint
        documents = await get_documents_by_date_range(
            start_date=config.start_date_str,
            end_date=config.end_date_str,
            session=session,
            limiter=limiter
        )

        # 3. filter for list of companies, to only keep the document list for valid
        filtered_docs = filter_documents(documents, doc_types=config.target_doc_types)

        # 4. download filtered documents concurrently (limiting to a subset if desired).
        downloaded_files = await download_documents(
            doc_list=filtered_docs,
            companies_to_get=config.companies_to_get,
            session=session,
            limiter=limiter
        )

        # 5. transform and Process all financial documents into a single DataFrame
        df_result = await process_financial_documents_concurrent(downloaded_files, company_info)

        if df_result.empty:
            logger.warning('No valid financial data found. Pipeline will terminate.')
            return
        
        # 6. Load and Export the final DataFrame to a CSV file
        load_to_csv(df_result, output_path='output', file_name='japan_company_data')

        logger.info('ETL pipeline completed successfully.')

if __name__ == '__main__':
    asyncio.run(main())