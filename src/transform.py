import os
import re
import xml.etree.ElementTree as ET

import pandas as pd
from tqdm import tqdm

from .utils.logger import logger


def parse_xbrl_file(file_path: str) -> pd.DataFrame:
    """
    Parse XBRL file to extract revenue data.
    The revenue data appears right after the NumberOfSubmissionDEI element.

    We keep a progress bar to track progress, but rely on logger for messages.
    """
    logger.info(f'Extracting data from {os.path.basename(file_path)}...')

    tree = ET.parse(file_path)
    root = tree.getroot()

    # XBRL uses namespaces, we need to handle them
    namespaces = dict(
        [node for _, node in ET.iterparse(file_path, events=['start-ns'])]
    )

    for prefix, uri in namespaces.items():
        ET.register_namespace(prefix, uri)

    elements = list(root.iter())
    total_elements = len(elements)

    for elem in elements:
        if elem.tag.endswith('CurrentPeriodEndDateDEI'):
            if elem.text:
                try:
                    date_str = elem.text
                    date_obj = pd.to_datetime(date_str).date()
                    fiscal_year = date_obj.year
                    break  # Found it, we can stop
                except Exception as e:
                    logger.info(
                        f"Failed to parse fiscal year end date '{elem.text}' in {file_path}: {e}"
                    )

    records = []
    for i, elem in enumerate(elements):
        if 'NumberOfSubmissionDEI' in elem.tag:
            # Get the next 5 elements after the marker
            for j in range(1, 6):
                if i + j < total_elements:
                    revenue_elem = elements[i + j]
                    context_ref = revenue_elem.get('contextRef')
                    if context_ref and revenue_elem.text:
                        record = {
                            'コンテキストID': context_ref,
                            '値': revenue_elem.text,
                            'ユニットID': revenue_elem.get('unitRef', 'JPY'),
                        }
                        records.append(record)
            break

    if not records:
        logger.info(
            f'No revenue data found in XBRL file: {os.path.basename(file_path)}'
        )

    return pd.DataFrame(records), fiscal_year


def parse_csv_file(file_path: str) -> pd.DataFrame:
    """
    Parse CSV file to extract revenue data.
    The first 5 rows contain historical revenue data.
    We keep a progress bar to track progress, but rely on logger for messages.
    """
    logger.info(f'Extracting data from {os.path.basename(file_path)}...')

    df = pd.read_csv(file_path, sep='\t', encoding='utf-16')

    # filter for revenue, the second row starts with revenue values, due to inconsistency in naming, filter based on the name of second row.
    revenue_mask = df.loc[1].get('要素ID')
    df_revenue = df[df['要素ID'] == revenue_mask]

    # Extract revenue data (rows 1-6 contain historical revenue data), now even if there is less than 5 rows for a company, it will be handled
    revenue_data = df_revenue[1:6].copy()
    year_mask = df['要素ID'] == 'jpdei_cor:CurrentFiscalYearEndDateDEI'
    fiscal_end_str = df.loc[year_mask, '値'].iloc[0]
    fiscal_end_date = pd.to_datetime(fiscal_end_str)
    fiscal_year = fiscal_end_date.year
    if len(revenue_data) == 0:
        logger.info(f'No revenue data found in CSV file: {os.path.basename(file_path)}')

    return revenue_data, fiscal_year


def select_best_files_by_company(file_paths: list) -> list:
    """
    Select the best file for each company (EDINET code) based on document type priority.
    Document type 130 (corrected version) has priority over 120.

    Args:
        file_paths: List of paths to financial documents
    Returns:
        List of selected file paths (one per company)
    """
    company_files = {}
    pattern = re.compile(r'E(\d+)_\d+_(\d+)\.(csv|xbrl)')

    with tqdm(
        total=len(file_paths), desc='Selecting best files', unit='file'
    ) as progress_bar:
        for file_path in file_paths:
            filename = os.path.basename(file_path)
            match = pattern.search(filename)
            if match:
                company_code = f'E{match.group(1)}'
                doc_type = match.group(2)

                # If company not in dict or doc_type == 130 (priority over 120)
                if company_code not in company_files or doc_type == '130':
                    company_files[company_code] = file_path
                    if doc_type == '130':
                        logger.info(
                            f'Using corrected file (type 130) for company {company_code}'
                        )
            progress_bar.update(1)

    selected_files = list(company_files.values())
    logger.info(
        f'Selected {len(selected_files)} unique company files from {len(file_paths)} total files'
    )
    return selected_files


def transform_financial_data(
    file_path: str, company_info: pd.DataFrame
) -> pd.DataFrame:
    """
    Transform the extracted financial data into the desired format.

    Args:
        file_path: Path to the extracted CSV/XBRL file (format: E{CODE}_YYYYMMDD_TYPE.csv/xbrl)
        company_info: DataFrame containing company information indexed by EDINET code
    Returns:
        DataFrame with columns: [year, companyname, industryclassification, geonameen, revenue, revenue_unit]
    """
    filename = os.path.basename(file_path)
    edinet_code = filename.split('_')[0]

    edinet_codes_set = set(company_info['EDINET Code'])

    if edinet_code not in edinet_codes_set:
        logger.info(f'EDINET code not found: {edinet_code}')
        return pd.DataFrame()

    company_data = company_info[company_info['EDINET Code'] == edinet_code]

    year_mapping = {
        'CurrentYearDuration': 0,
        'Prior1YearDuration': -1,
        'Prior2YearDuration': -2,
        'Prior3YearDuration': -3,
        'Prior4YearDuration': -4,
    }

    # Determine how to parse based on extension
    if file_path.endswith('.csv'):
        revenue_data, fiscal_year = parse_csv_file(file_path)
    elif file_path.endswith('.xbrl'):
        revenue_data, fiscal_year = parse_xbrl_file(file_path)
    else:
        logger.info(f'Unsupported file format: {file_path}')
        return pd.DataFrame()

    records = []
    for _, row in revenue_data.iterrows():
        relative_year = row['コンテキストID']
        year_offset = year_mapping.get(relative_year)
        if year_offset is None:
            logger.info(f'Skipping unknown year indicator: {relative_year}')
            continue
        actual_year = fiscal_year + year_offset

        try:
            revenue_int = int(row['値'])
        except ValueError:
            logger.info(
                f"Skipping row with invalid revenue: '{row['値']}' for year {actual_year}"
            )
            continue

        record = {
            'year': actual_year,
            'companyname': company_data['Submitter Name（alphabetic）'].iloc[0],
            'industryclassification': company_data["Submitter's industry"].iloc[0],
            'geonameen': 'Japan',
            'revenue': revenue_int,
            'revenue_unit': row['ユニットID'],
        }
        records.append(record)

    return pd.DataFrame(records)


def process_financial_documents(
    file_paths: list, company_info: pd.DataFrame
) -> pd.DataFrame:
    """
    Process multiple financial documents and combine them into a single DataFrame.

    Args:
        file_paths: List of paths to financial documents
        company_info: DataFrame containing company information with EDINET Code column
    Returns:
        Combined DataFrame with all financial data
    """
    selected_files = select_best_files_by_company(file_paths)

    dfs = []
    with tqdm(
        total=len(selected_files), desc='Processing files', unit='file'
    ) as progress_bar:
        for file_path in selected_files:
            df = transform_financial_data(file_path, company_info)
            if not df.empty:
                dfs.append(df)
            progress_bar.update(1)

    if not dfs:
        logger.info('No valid data frames to combine')
        return pd.DataFrame()

    # Show progress bar for big merges
    if len(dfs) > 100:
        with tqdm(total=1, desc='Combining results', unit='operation') as pbar:
            result_df = pd.concat(dfs, ignore_index=True)
            pbar.update(1)
    else:
        result_df = pd.concat(dfs, ignore_index=True)

    return result_df
