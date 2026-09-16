
from dataclasses import dataclass
from src.common.regex_patterns import REGEX_PATTERNS

def get_reason(df: DataFrame) -> DataFrame:
    control_col = [col_name for col_name in df.columns if col_name.startwith("_") and col_name != "_sk"]
    data_col = [col_name for col_name in df.columns if not col_name.startwith("_")]
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
class Silverlayer:
    pipeline_name: str
    table_name: str
    shema_details: dict[str, str]
    write_mode: str

    def __post_init__(self) -> None:
        self.table_name_bronze = (f"{self.table_name}_bronze")
        self.table_name_silver = (f"{self.table_name}_silver")
        self.table_name_bad_record = (f"{self.table_name}_bad_record")
        
        
    def read_data(self) -> Dataframe:
        return (
            spark.table(self.table_name_bronze)
            .select(
                *data_col,
                monotonically_increasing_id().alias("_sk")
            )
        )
    def get_invalid_record(self, bronze_df:Dataframe) -> Dataframe:
        invalid_col = {
            f"_is_{col_name}_invalid": coalesce(~col(col_name).rlike(REGEX_PATTERNS[col_type]), lit(False))
            for col_name, col_type in self.shema_details.items() if col_type != "string"
        }
        invalid_df = (
            bronze_df
            .withColumns(invalid_col)
            .transform(get_reason)
        )
        return 