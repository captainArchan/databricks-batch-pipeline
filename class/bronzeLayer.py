
from dataclasses import dataclass, field
from pyspark.sql.functions import *
from pyspark.sql.dataframe import *
from abc import ABC, abstractmethod


class IFileReaderStrategy(ABC):
    @abstractmethod
    def get_dataframe(self) -> DataFrame:
        pass

    
@dataclass
class CsvDataExtractor(IFileReaderStrategy):
    delimiter: str
    header: bool
    multiline: bool
    escape_option: str
    quote_option: str
    file_path: str
    def get_dataframe(self)-> DataFrame:
        return(
            spark.read.format("csv")
            .option("header", self.header)
            .option("delimiter", self.delimiter)
            .option("quote", self.quote_option)
            .option("escape", self.escape_option)
            .option("multiLine", self.multiline)
            .load(self.file_path)
        )

class ExtractorFactory:
    @staticmethod
    def create(strategy_type: str, **kwarges) -> IFileReaderStrategy:
        strategy_type = strategy_type.lower()
        if strategy_type == "csv":
            return CsvDataExtractor(**kwarges)
        else:
            raise ValueError("")

@dataclass
class BronzeLayer:
    pipeline_name: str
    file_path: str
    table_name: str
    write_mode:str
    format_data: str
    reader_options: dict = field(default_factory=dict)
    
    def __post_init__(self) -> None:
        self.file_format = self.file_path.split(".")[-1]
        self.table_name_bronze = (f"{self.table_name}_bronze")
    
    def read_from_raw(self) -> DataFrame:
        try:
            reader = ExtractorFactory.create(self.file_format, **self.reader_options)
            df_raw = reader.get_dataframe()
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
        except:
            raise ValueError("")

    def write_file(self, df:DataFrame) -> bool:
        (
            df
            .write
            .format(self.format_data)
            .mode(self.write_mode)
            .saveAsTable(self.table_name_bronze)
        )
