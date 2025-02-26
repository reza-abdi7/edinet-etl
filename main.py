from config.config import config
from src.extract import (
    download_documents,
    extract_companies,
    filter_documents,
    get_documents_by_date_range,
)
from src.load import load_to_csv
from src.transform import process_financial_documents
from src.utils.logger import logger


def main():
    # 1. Load or construct your company_info DataFrame
    company_info = extract_companies()

    # 2. get list of documents for date range from List Retrieval API endpoint
    list_docs = get_documents_by_date_range(
        start_date=config.start_date_str, end_date=config.end_date_str
    )

    # 3. filter for list of companies, to only keep the document list for valid
    filtered_doc_list = filter_documents(list_docs, doc_types=config.target_doc_types)

    # 4. download filtered documents
    downloaded_files = download_documents(
        doc_list=filtered_doc_list, companies_to_get=config.companies_to_get
    )

    # 5. transform and Process all financial documents into a single DataFrame
    df_result = process_financial_documents(downloaded_files, company_info)

    if df_result.empty:
        logger.warning('No valid financial data found. Pipeline will terminate.')
        return

    # 6. Load and Export the final DataFrame to a CSV file
    load_to_csv(df_result, output_path='output', file_name='japan_company_data')

    logger.info('ETL pipeline completed successfully.')


if __name__ == '__main__':
    main()
