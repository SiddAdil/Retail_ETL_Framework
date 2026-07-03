from pyspark.sql import DataFrame


class Writer:

    @staticmethod
    def write(
        df: DataFrame,
        path: str,
        output_format: str,
        mode: str = "overwrite",
        partition_columns: list[str] | None = None
    ) -> None:

        writer = (
            df.write
            .format(output_format)
            .mode(mode)
        )

        if partition_columns:
            writer = writer.partitionBy(*partition_columns)

        writer.save(path)