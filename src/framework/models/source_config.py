"""
Module: source_config.py

Purpose:
Stores metadata required to process a source dataset.
"""

from dataclasses import dataclass
from typing import List


@dataclass
class SourceConfig:
    """
    Configuration for a source dataset.
    """

    source_name: str

    input_path: str

    bronze_path: str

    silver_path: str

    business_key: List[str]

    order_column: str

    load_timestamp_column: str = "load_timestamp"

    file_format: str = "csv"

    output_format: str = "parquet"

    partition_columns: List[str] | None = None