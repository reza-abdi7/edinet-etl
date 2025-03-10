from typing import List

from pydantic_settings import BaseSettings, SettingsConfigDict


class Config(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=[
            'config/settings.env',  # env file for general settings of the codebase
            '.env',  # env file for secret key
        ],
        env_file_encoding='utf-8',
    )
    api_key: str
    base_url: str
    csv_file: str
    output_dir: str
    target_doc_types: List[str]
    start_date_str: str
    end_date_str: str
    request_per_second: float
    max_retries: int
    retry_delay: float
    max_concurrent_requests: int
    companies_to_get: int


config = Config()
