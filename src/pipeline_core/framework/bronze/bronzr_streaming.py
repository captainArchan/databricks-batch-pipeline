from dataclasses import dataclass
from src.pipeline_core.framework.bronze.bronze_layer_template_method import BronzeDataPipeline

@dataclass
class BronzeStreaming(BronzeDataPipeline):
    file_path: str
    table_name: str
    write_mode:str
    format_data: str
    spark: SparkSession
    reader_options: dict = field(default_factory=dict)
    encrypt_col: list[str] = field(default_factory=list)
    
    def __post_init__(self) -> None:
        self.file_format = self.file_path.split(".")[-1]
        self.table_name_bronze = (f"batch_project.{self.table_name}_bronze")
    
    def read_data(self) -> DataFrame:
        return (
            self.spark.readStream
            .format(self.file_format)
            .schema()
            .option("cleanSource", "ARCHIVE")
            .option("sourceArchiveDir", f"/Volumns/bronze/streaming/archive_{self.table_name}")
            .load(self.file_path)
        )
    
    def write_data(self, df) -> DataFrame:
        (
            df.writeStream
            .outputMode("append")
            .option("checkpointLocation", "")
            .trigger(avaliableNow=True)
            .table(self.table_name_bronze)
        )
        return
    