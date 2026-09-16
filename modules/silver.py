
from dataclasses import dataclass


@dataclass
class Silverlayer:
    pipeline_name: str
    table_name: str

    def __post_init__(self) -> None:
        self.table_name_bronze = (f"{self.table_name}_bronze")
    
    def read_data(self) -> Dataframe:
        return (
            spark.table(self.table_name_bronze)
            .select(
                *data_col,
                monotonically_increasing_id().alias("_sk")
            )
        )