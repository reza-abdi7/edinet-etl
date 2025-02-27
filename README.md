# EDINET ETL Pipeline

An ETL pipeline that extracts, transforms, and loads corporate financial data from Japan's EDINET (Electronic Disclosure for Investors' NETwork) API.
The API has two endpoints for 1. Retrieving List of submitted documents for a specific date, and 2. Retrieving/downloading the document needed based on docID retrieved from List of submitted documents.

## Project Overview

This pipeline extracts financial data for Japanese listed companies from the EDINET system, processes and transforms it into a structured format, and outputs the data to CSV files for further analysis. The pipeline is designed to be configurable, reliable, and easy to maintain.

### Links

- **Account Login/Signup**: [EDINET API Auth Page](https://api.edinet-fsa.go.jp/api/auth/index.aspx?mode=1)
- **API Documentation**: [EDINET API Documentation Link](https://disclosure2dl.edinet-fsa.go.jp/guide/static/disclosure/download/ESE140206.zip) (only in Japenese)
- **EDINET Codes (official)**: [EDINET Code Search](https://disclosure2.edinet-fsa.go.jp/weee0020.aspx)
in the bottom of the page
![alt text](data/image.png)

## Scope

- **Data Extraction**: Retrieves company information and financial documents from the EDINET API.
- **Data Transformation**: Processes and structures the raw financial data per company, focusing on revenue information only (can be extended further to other KPIs).
- **Data Loading**: Outputs the processed data to CSV files desired.

## Features

### Functionalities
- Fetches and processes data from EDINET API with configurable date ranges
- Filters documents based on document types and company criteria
- Handles rate limiting and implements retry mechanisms for API calls avoiding hitting ratelimit and error 429
- Extracts financial information from both XBRL and CSV documents (currently configured as csv priorority, if csv does not exist will continue with XBRL)
- Logs job execution details for monitoring and debugging
- Selects the best available documents when multiple versions exist for a company

### Tech Stack
- **Python**: Core programming language
- **Pandas**: Data manipulation and transformation
- **Pydantic**: Configuration management with environment variables
- **Requests**: HTTP client for API calls
- **Requests-RateLimiter**: Rate limiting for API requests
- **tqdm**: Progress tracking for long-running operations
- **XML ElementTree**: Parsing XBRL files

### Dev Tools
- **Ruff**: Fast Python linter and code formatter used to ensure code quality and consistency
- **Pre-commit**: Git hook framework to run code quality checks before commits, preventing issues from being added to the codebase


## ETL Pipeline Diagram / Workflow
### ETL Steps

![ETL Pipeline Diagram](data\etl_diagram.png)


1. **Extract**:
   - Extract company information from EDINET company list CSV
   - Get document list for specified date range from EDINET API
   - Filter documents by type and company criteria
   - Download relevant financial documents using List of document's Metadata retrieved from last step
   - Save binary file to process (the response from API is binary)

2. **Transform**:
   - Parse downloaded XBRL/CSV documents
   - Select best statement for a company if multiple file available for a company
   - Extract revenue information
   - Structure data with company details, fiscal years, and financial metrics

3. **Load**:
   - Save processed data to CSV files in the specified output directory

### Folder Structure
```
edinet-etl/
├── config/                  # Configuration files
│   ├── config.py            # Pydantic configuration class
│   └── settings.env         # Environment variables
├── data/                    # Input data files (EDINET company list)
├── src/                     # Source code
│   ├── extract.py           # Data extraction modules
│   ├── transform.py         # Data transformation modules
│   ├── load.py              # Data loading modules
│   └── utils/               # Utility functions
│       ├── helper.py        # Helper functions
│       └── logger.py        # Logging configuration
├── tests/                   # Unit and integration tests
├── .env                     # Secret environment variables (API keys)
├── main.py                  # Main pipeline execution script
└── requirements.txt         # Project dependencies
```

## Installation & Setup

### Requirements
- Python 3.8 or higher
- Dependencies listed in requirements.txt

### Clone the repo
```bash
git clone https://github.com/reza-abdi7/edinet-etl.git
```

### Installation
```bash
pip install -r requirements.txt
```

### Environment Variables
The pipeline uses two environment files:
- `config/settings.env`: General configuration settings
- `.env`: Secret API keys and credentials

Create an API key by signing up 

## Configuration

### How to Customize
Edit the environment variables in `config/settings.env` to customize:
- Date ranges for data extraction
- Document types to retrieve
- Number of companies to process
- Rate limiting parameters

### Config Explanation
Key configuration variables include:
- `API_KEY`: Your EDINET API key
- `BASE_URL`: The EDINET API base URL
- `TARGET_DOC_TYPES`: Document types to retrieve (e.g., ["120"] for annual securities reports)
- `START_DATE_STR` / `END_DATE_STR`: Date range for document retrieval
- `COMPANIES_TO_GET`: Maximum number of companies to process (if None, it will get all companies available in list)
- `REQUEST_PER_SECOND`: Rate limiting for API requests

## Usage

### Running the ETL Pipeline
To run the complete pipeline:
```bash
python main.py
```

### Output
The pipeline generates a CSV file containing structured financial data in the `output` directory (configurable).

## TODOs for Future Improvements
- Implement Async and Concurrent requests for faster document retrieval
- Add multithreading for faster document processing
- Add more robust data validation and quality checks
- Extend to extract additional financial metrics beyond revenue
- Add more tests
