"""
Module: bronze_loader.py

Purpose:
Loads raw source data into the Bronze layer by adding
technical metadata while preserving the original data.
"""

from pyspark.sql import DataFrame

from pyspark.sql.functions import (
    current_timestamp,
    current_date,
    input_file_name,
    regexp_extract,
    lit,
    col
)

from framework.models.source_config import SourceConfig

class BronzeLoader:
    """
    Adds Bronze layer metadata to the incoming dataset.
    """

    def load(
        self,
        df: DataFrame,
        source_config: SourceConfig,
        batch_id: str
        ) -> DataFrame:

            
        """
        Prepare a Bronze DataFrame by adding technical metadata.

        Parameters
        ----------
        df : DataFrame
            Source DataFrame.

        source_config : SourceConfig
            Source metadata.

        batch_id : str
            Pipeline execution batch id.

        Returns
        -------
        DataFrame
            Bronze DataFrame.
        """

        self._validate_input(df, source_config, batch_id)

        bronze_df = self._add_metadata(
            df=df,
            source_config=source_config,
            batch_id=batch_id
        )

        return bronze_df

    def _validate_input(
        self,
        df: DataFrame,
        source_config: SourceConfig,
        batch_id: str
        ) -> None:
            
        """
        Validate BronzeLoader inputs.
        """

        if df is None:
            raise ValueError("Input DataFrame cannot be None.")

        if source_config is None:
            raise ValueError("SourceConfig cannot be None.")

        if not batch_id:
            raise ValueError("Batch ID cannot be empty.")

    def _add_metadata(
        self,
        df: DataFrame,
        source_config: SourceConfig,
        batch_id: str
        ) -> DataFrame:               
        bronze_df = (
            df
            .withColumn(
                "load_timestamp",
                current_timestamp()
                )
            .withColumn(
                "ingestion_date",
                current_date()
                )
            
            .withColumn(
                "batch_id",
                lit(batch_id)
            )
            .withColumn(
                "record_source",
                lit(source_config.record_source)
            )
            .withColumn(
                "source_file_name",
                regexp_extract(
                    col("_metadata.file_path"),
                    r"([^/]+$)",
                    1
                )
            )
        )

        return bronze_df