
from dataclasses import dataclass
from pyspark.sql import DataFrame


class BaseProcessor:
    def __init__(self, spark):
        self.spark = spark
    
    def write_scd_type_1(self, source_df: DataFrame, target_df: DataFrame, merge_keys:list[str], value_keys: list[str]):    
        
        source = (
            source_df
            .withColumn("hash_key", xxhash64(*merge_keys))
            .withColumn("hash_value", xxhash64(*value_keys))
        )

        target = (
            target_df
            .withColumn("hash_key", xxhash64(*merge_keys))
            .withColumn("hash_value", xxhash64(*value_keys))
        )

        log_df = (
            target_df
            .alias("target")
            .merge(
                source = source_df.alias("source"),
                condition = "target.hash_key = source.hash_key"
            ).whenMatchUpdateAll(
                condition="target.hash_value != source.hash_value"
                
            )
            .whenNotMatchInsertAll()
        ).execute()
    
    def write_scd_type_2(self, df_source, target_table: str, merge_keys: list):
        return