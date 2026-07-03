"""
Module: reader.py

Purpose:
Generic reader for loading datasets.
"""

from pyspark.sql import SparkSession

from framework.models.source_config import SourceConfig


class Reader:

    @staticmethod
    def read(
        spark: SparkSession,
        source_config: SourceConfig
    ):
        """
        Read data based on SourceConfig.
        """

        return (
            spark.read
            .format(source_config.file_format)
            .option("header", True)
            .option("inferSchema", True)
            .load(source_config.input_path)
        )