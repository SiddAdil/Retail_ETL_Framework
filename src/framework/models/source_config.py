"""
Module: source_config.py

Purpose:
Stores metadata required to process a source dataset.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class SourceConfig:
    """
    Configuration for a source dataset.
    """

    source_name: str

    input_path: str

    bronze_path: str

    silver_path: str

    business_key: list[str]

    order_column: str

    record_source: str

    gold_path: str

    load_timestamp_column: str = "load_timestamp"

    file_format: str = "csv"

    output_format: str = "parquet"

    write_mode: str = "overwrite"

    customer_name_column: str = "customer_name"

    partition_columns: list[str] | None = None
    
    