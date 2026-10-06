from dataclasses import dataclass, field
from pyspark.sql.functions import *
from pyspark.sql.dataframe import *
from abc import ABC, abstractmethod
from src.pipeline_core.framework.bronze.extract_data import ExtractorFactory
from pyspark.sql import SparkSession

@dataclass
class BronzeLayer:
    file_path: str
    table_name: str
    write_mode:str
    format_data: str
    spark: SparkSession
    reader_options: dict = field(default_factory=dict)
    encrypt_col: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        self.file_format = self.file_path.split(".")[-1]
        self.table_name_bronze = (f"{self.table_name}_bronze")

    @classmethod
    def from_config_table(cls, spark: SparkSession, pipeline_name: str) -> "BronzeLayer":
        conf = (
            spark.table("session_11_framework.config_table")
            .filter(col("pipeline_name") == pipeline_name)
            .select(
                "encrypt_col",
                "table_name", 
                "file_path",
                "source_option"
                )
            ).first()
        
        return cls(
            file_path= conf.file_path,
            table_name= conf.table_name,
            write_mode= "append",
            format_data="delta",
            encrypt_col= conf.encrypt_col,
            reader_options=conf.source_option,
            spark=spark
        )
    
    def read_from_raw(self) -> DataFrame:
        reader = ExtractorFactory.create(self.file_format, self.spark, **self.reader_options)
        df_raw = reader.get_dataframe(self.file_path)
        df_with_audit = (
            df_raw
            .withColumn("_load_dt", current_date())
            .withColumn("_load_dttm", current_timestamp())
            .withColumn("_file_name", col("_metadata.file_name"))
            .withColumn("_file_path", col("_metadata.file_path"))
            .withColumn("_file_size", col("_metadata.file_size"))
            .withColumn("_file_mod", col("_metadata.file_modification_time"))
        )

        return df_with_audit


    def write_file(self, df:DataFrame) -> None :
            (
                df
                .write
                .format(self.format_data)
                .mode(self.write_mode)
                .saveAsTable(self.table_name_bronze)
            )
    
    def encrypt_df(self, df: DataFrame, encrypt_key: str) -> DataFrame:
        if self.encrypt_col:
            return (
                df
                .withColumns({
                    column: aes_encrypt(col(column), lit(encrypt_key), lit("ECB"), lit("PKCS"))  
                    for column in self.encrypt_col
                })
            )
        return df