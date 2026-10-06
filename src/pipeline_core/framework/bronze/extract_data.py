from dataclasses import dataclass, field
from pyspark.sql.functions import *
from pyspark.sql.dataframe import *
from abc import ABC, abstractmethod
from pyspark.sql import SparkSession

class IFileReaderStrategy(ABC):
    @abstractmethod
    def get_dataframe(self, file_path) -> DataFrame:
        pass

@dataclass
class CsvDataExtractor(IFileReaderStrategy):
    spark: SparkSession
    delimiter: str
    header: bool
    multiline: bool = False
    escape_option: str = ""
    quote_option: str = ""

    def get_dataframe(self, file_path) -> DataFrame:
        return(
            self.spark.read.format("csv")
            .option("header", self.header)
            .option("delimiter", self.delimiter)
            .option("quote", self.quote_option)
            .option("escape", self.escape_option)
            .option("multiLine", self.multiline)
            .load(file_path)
        )

@dataclass
class JsonDataExtractor(IFileReaderStrategy):
    spark: SparkSession
    header: bool
    multiline: bool
    primitivesAsString: bool
    allowComments: bool
    def get_dataframe(self, file_path) -> DataFrame:
        return (
            self.spark.read.format("json")
            .option("header", self.header)
            .option("multiline", self.multiline)
            .option("primitivesAsString", self.primitivesAsString)
            .option("allowComments", self.allowComments)
            .load(file_path)
        )

class ExtractorFactory:
    @staticmethod
    def create(strategy_type: str, spark: SparkSession, **kwarges) -> IFileReaderStrategy:
        strategy_type = strategy_type.lower()
        if strategy_type == "csv":
            return CsvDataExtractor(spark, **kwarges)
        elif strategy_type == "json":
            return JsonDataExtractor(spark, **kwarges)
        else:
            raise ValueError("")
