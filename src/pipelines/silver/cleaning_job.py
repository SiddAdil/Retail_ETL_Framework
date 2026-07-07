"""
Module: cleaning_job.py

Purpose:
Performs Silver layer data cleaning transformations.
"""

from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col,
    row_number,
    trim,
    regexp_replace,
    initcap,
    col
)

from framework.models.source_config import SourceConfig


class CleaningJob:
    """
    Executes Silver layer cleaning transformations.
    """

    _ROW_NUM_COL = "_row_num"

    def execute(
        self,
        df: DataFrame,
        source_config: SourceConfig
    ) -> tuple[DataFrame, DataFrame]:
        """
        Execute Silver layer cleaning.

        Parameters
        ----------
        df : DataFrame
            Input Bronze DataFrame.

        source_config : SourceConfig
            Source metadata.

        Returns
        -------
        tuple[DataFrame, DataFrame]
            - clean_df
            - reject_df
        """

        self._validate_input(df, source_config)

        clean_df, duplicate_df = self._remove_duplicates(
            df=df,
            source_config=source_config
        )

        # Future transformations will be added here
        #
        clean_df = self._apply_trim(
            clean_df, source_config)
        clean_df = self._standardize_customer_name(clean_df)
        clean_df = self._standardize_email(clean_df)
        clean_df, invalid_email_df  = self._validate_email(
            clean_df, source_config)
        # clean_df, phone_reject_df = self._validate_phone(clean_df)

        return clean_df, duplicate_df, invalid_email_df

    def _validate_input(
        self,
        df: DataFrame,
        source_config: SourceConfig
    ) -> None:
        """
        Validate CleaningJob inputs.
        """

        if df is None:
            raise ValueError("Input DataFrame cannot be None.")

        if source_config is None:
            raise ValueError("SourceConfig cannot be None.")

    def _remove_duplicates(
        self,
        df: DataFrame,
        source_config: SourceConfig
    ) -> tuple[DataFrame, DataFrame]:
        """
        Remove duplicate records based on business keys.

        Business Rule
        -------------
        - Keep the latest record based on order_column.
        - If order_column is the same, keep the latest load_timestamp.
        - Remaining records are considered duplicates.

        Parameters
        ----------
        df : DataFrame
            Input Spark DataFrame.

        source_config : SourceConfig
            Metadata containing business keys and order column.

        Returns
        -------
        tuple[DataFrame, DataFrame]
            clean_df:
                Latest records.

            duplicate_df:
                Duplicate records.
        """

        window_spec = (
            Window
            .partitionBy(*source_config.business_key)
            .orderBy(
                col(source_config.order_column).desc_nulls_last(),
                col("load_timestamp").desc()
            )
        )

        ranked_df = (
            df.withColumn(
                self._ROW_NUM_COL,
                row_number().over(window_spec)
            )
        )

        clean_df = (
            ranked_df
            .filter(col(self._ROW_NUM_COL) == 1)
            .drop(self._ROW_NUM_COL)
        )

        duplicate_df = (
            ranked_df
            .filter(col(self._ROW_NUM_COL) > 1)
            .drop(self._ROW_NUM_COL)
        )

        return clean_df, duplicate_df
    
    def _standardize_customer_name(
        self,
        df: DataFrame,
        source_config: SourceConfig
    ) -> DataFrame: 
    """
    Standardize customer names.

    Rules
    -----
    - Remove leading/trailing spaces.
    - Replace multiple spaces with a single space.
    - Convert to Proper Case.
    """

    return (
        df.withColumn(
            source_config.customer_name_column,
            initcap(
                        col(source_config.customer_name_column)
                    )
        )
    )


    def _standardize_email(
        self,
        df: DataFrame
    ) -> DataFrame :
        """
    Standardize email addresses.

    Rules
    -----
    - Remove leading/trailing spaces.
    - Convert to lowercase.
    """

    return (
        df.withColumn(
            "email",
            lower(
                    col("email")
                
            )
        )
    )

    def _apply_trim(

        self,
        df: DataFrame,
        source_config: SourceConfig
    ) -> DataFrame:
    """
    Trim leading and trailing whitespace from configured columns.
    """

    if not source_config.trim_columns:
        return df

    for column_name in source_config.trim_columns:

        df = (
            df.withColumn(
                column_name,
                trim(col(column_name))
            )
        )

    return df