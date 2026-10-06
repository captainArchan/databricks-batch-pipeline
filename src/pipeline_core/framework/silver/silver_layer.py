from dataclasses import dataclass
from src.common.regex_patterns import REGEX_PATTERNS
from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import *
from pyspark.sql.dataframe import *
from src.common.base_processor import WriteStrategyFactory
from pyspark.sql import SparkSession

def get_reason(df: DataFrame) -> DataFrame:

    control_col = [col_name for col_name in df.columns if col_name.startswith("_") and col_name != "_sk"]
    data_col = [col_name for col_name in df.columns if not col_name.startswith("_")]
    or_statement = " OR ".join([col_name for col_name in control_col])
    
    return (
        df
        .filter(or_statement)
        .melt(
            ids = [*data_col, "_sk"],
            values = control_col,
            variableColumnName = "reason",
            valueColumnName = "status"
        )
        .filter(col("status") == True)
        .groupBy(*data_col, "_sk")
        .agg(collect_list("reason").alias("reason"))
    )

@dataclass
class SilverLayer:
    table_name: str
    schema_details: dict[str, str]
    keys: list[str]
    write_mode: str
    spark: SparkSession

    def __post_init__(self) -> None:
        self.table_name_bronze = (f"{self.table_name}_bronze")
        self.table_name_silver = (f"{self.table_name}_silver")
        self.table_name_bad_record = (f"{self.table_name}_bad_record")
        self.data_col = [col_name for col_name in self.schema_details.keys()]
    
    @classmethod
    def from_config_table(cls, spark: SparkSession,pipeline_name: str)-> "SilverLayer":
        conf = (
            spark.table("session_11_framework.config_table")
            .filter(col("pipeline_name") == pipeline_name)
            .select(
                "table_name",
                "schema_details",
                "keys",
                "write_mode"
            )
        ).first()

        return cls(
            table_name = conf.table_name,
            schema_details = conf.schema_details,
            keys = conf.keys,
            write_mode = conf.write_mode,
            spark = spark
        )


    def read_data(self) -> DataFrame:
        df = self.spark.table(self.table_name_bronze)
        max_time = df.select(max("_load_dttm")).first()[0]
        return (
            self.spark.table(self.table_name_bronze)
            .select(
                *self.data_col,
                monotonically_increasing_id().alias("_sk")
            )
            .filter(
                col("_load_dttm") == max_time
            )
        )
    
    def get_invalid_record(self, df: DataFrame) -> DataFrame:
        invalid_col = {
            f"_is_{col_name}_invalid": coalesce(~col(col_name).rlike(REGEX_PATTERNS[col_type]), lit(False))
            for col_name, col_type in self.schema_details.items() if col_type != "string"
        }
        invalid_df = (
            df
            .withColumns(invalid_col)
            .transform(get_reason)
        )
        return invalid_df
    
    def get_key_null_record(self, df: DataFrame) -> DataFrame:
        key_null_statement = {
            f'_is_{col_name}_null': col(col_name).isNull() for col_name in self.keys
        }
        key_null_df = (
            df
            .withColumns(key_null_statement)
            .transform(get_reason)
        )
        
        return key_null_df
        
    def get_dup_record(self, df: DataFrame, key_null_df: DataFrame) -> DataFrame:
        partition_by_all = Window.partitionBy(*self.data_col,).orderBy("_sk")
        partition_by_key = Window.partitionBy(*self.keys)
        
        df_not_null = df.join(
            key_null_df,
            ['_sk'],
            "left_anti"
        )
        is_row_duplicated_df = (
            df_not_null
            .withColumn("rn", row_number().over(partition_by_all))
            .filter(col("rn")>1)
            .drop("rn")
            .withColumn("reason", array(lit("_row_duplicated")))
        )
        
        is_key_duplicated_df = (
            df_not_null
            .join(
                is_row_duplicated_df,
                ['_sk'],
                "left_anti"
            )
            .withColumn("count", count("*").over(partition_by_key))
            .filter(col("count") > 1)
            .drop("count")
            .withColumn("reason", array(lit("_key_duplicated")))
        )

        duplicate_df = (
            is_row_duplicated_df
            .union(is_key_duplicated_df)
        )
        
        return duplicate_df
        
    def get_all_bad_records(self, invalid_df: DataFrame, key_null_df: DataFrame, duplicate_df: DataFrame) -> DataFrame:
        bad_df = (
            invalid_df
            .unionByName(key_null_df)
            .unionByName(duplicate_df)
            .groupBy(*self.data_col, "_sk")
            .agg(flatten(collect_list("reason")).alias("reason"))
        )

        return bad_df
    
    def get_cleansing_df(self, df: DataFrame, bad_df: DataFrame) -> DataFrame:
        add_control_col = {
            "_load_dt": current_date(),
            "_load_dttm": current_timestamp(),
        }
        cast_statement = [
            col(col_name).cast(col_type)
            for col_name, col_type in self.schema_details.items()
        ]
        
        cleansing_df = (
            df
            .join(bad_df, "_sk", "left_anti")
            .select(*cast_statement)
            .withColumns(add_control_col)
        )
        return cleansing_df
    
    def load_bad_record(self, bad_df: DataFrame)-> None:
        (
            bad_df
            .write
            .mode("append")
            .saveAsTable(self.table_name_bad_record)
        )
    def return_bad_db(self) -> DataFrame:
        return self.spark.table(self.table_name_bad_record)
        
    def load_to_silver_layer(self, cleaned_df: DataFrame) -> None:
        base_processor = WriteStrategyFactory.create(
            write_mode = self.write_mode,
            spark = self.spark,
            source_df = cleaned_df,
            target_table = self.table_name_silver,
            merge_keys = self.keys,
            value_keys = self.data_col
        )
        base_processor.save_data()
