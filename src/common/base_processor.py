
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pyspark.sql import DataFrame, SparkSession
from delta.tables import DeltaTable
from pyspark.sql.functions import col, lit, xxhash64, when
from datetime import date


class BaseProcessor(ABC):
    @abstractmethod
    def save_data(self) -> None:
        pass

@dataclass
class SCDType2(BaseProcessor):
    spark: SparkSession
    source_df: DataFrame
    target_table: str
    merge_keys: list[str]
    value_keys: list[str]

    def save_data(self) -> None:
        source_df = (
            self.source_df
            .withColumn("hash_key", xxhash64(*self.merge_keys))
            .withColumn("hash_value", xxhash64(*self.value_keys))
        )
        if self.spark.catalog.tableExists(self.target_table):
            source_updated = source_df.select("*", lit(None).cast("date").alias("end_date"))
            (
                source_updated.write
                .format("delta")
                .mode("overwrite")
                .saveAsTable(self.target_table)
            ) 
            return  
        
        target_df = (
            spark.table(self.target_table)
            .filter(col("end_date") is None)   
        )

        source_transform_df = (
            source_df.alias("source")
            .join(target_df.alias("target"), 
                  [col("source.hash_key") == col("target.hash_key")], "left")
            .withColumn("record_status", 
                        when(col("target.hash_key").isNull(), lit("insert"))
                        .when(col("target.hash_value") != col("source.hash_value"), lit("change"))
                        .otherwise(lit("no_change"))
                        )
            .select("source.*", "record_status")
        )

        insert_df = (
            source_transform_df
            .filter(col("record_status") == 'insert')
            .withColumn("merge_key", lit(None))
        )

        change_df = (
            source_transform_df
            .filter(col("record_status") == "change")
        )

        final_change_df = (
            change_df.withColumn("merge_key", lit(None))
            .unionByName(change_df.withColumn("merge_key", col("hash_key")))
        )

        result_df = (
            insert_df.unionByName(final_change_df)
        )

        delta_table = DeltaTable.forName(spark, self.target_table)
        process = (
            delta_table.alias("target")
            .merge(result_df.alias("source"), "target.hash_key = source.hash_key")
            .whenMatchedUpdate(
                set = {
                    "end_date": date.today()
                },
                condition = col("target.end_date") is None
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
        

    
@dataclass
class SCDType1(BaseProcessor):
    spark: SparkSession
    source_df: DataFrame
    target_table: str
    merge_keys: list[str]
    value_keys: list[str]
        
    def save_data(self) -> None:        
        source = (
            self.source_df
            .withColumn("hash_key", xxhash64(*self.merge_keys))
            .withColumn("hash_value", xxhash64(*self.value_keys))
        )

        if not self.spark.catalog.tableExists(self.target_table):
            print(f"ไม่พบตาราง {self.target_table} ระบบจะสร้างใหม่ (Initial Load)")
            (
                source.write
                .format("delta")
                .mode("overwrite")
                .saveAsTable(self.target_table)
            )
            return
        target_delta = DeltaTable.forName(self.spark, self.target_table)
        log_df = (
            target_delta
            .alias("target")
            .merge(
                source = self.source_df.alias("source"),
                condition = "target.hash_key = source.hash_key"
            ).whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
        ).execute()


class WriteStrategyFactory():
    @staticmethod
    def create(write_mode: str, **kwarges) -> BaseProcessor:
        write_mode = write_mode.lower()
        if write_mode == "scd1":
            return SCDType1(**kwarges)
        
# class BaseProcessor:
#     def __init__(self, spark):
#         self.spark = spark
    
#     def write_scd_type_1(self, source_df: DataFrame, target_df: DataFrame, merge_keys:list[str], value_keys: list[str]):    
        
#         source = (
#             source_df
#             .withColumn("hash_key", xxhash64(*merge_keys))
#             .withColumn("hash_value", xxhash64(*value_keys))
#         )

#         target = (
#             target_df
#             .withColumn("hash_key", xxhash64(*merge_keys))
#             .withColumn("hash_value", xxhash64(*value_keys))
#         )

#         log_df = (
#             target_df
#             .alias("target")
#             .merge(
#                 source = source_df.alias("source"),
#                 condition = "target.hash_key = source.hash_key"
#             ).whenMatchUpdateAll(
#                 condition="target.hash_value != source.hash_value"
#             )
#             .whenNotMatchInsertAll(
                
#             )
#         ).execute()
    
#     def write_scd_type_2(self, df_source, target_table: str, merge_keys: list):
#         return