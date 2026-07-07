from dataclasses import dataclass
from pyspark.sql import DataFrame

@dataclass
class PipelineResult:
    clean_df: DataFrame

    reject_dfs: dict[str, DataFrame]

    