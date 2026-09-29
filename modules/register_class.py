from pyspark.sql.types import *
from delta.tables import DeltaTable
from pyspark.sql.functions import *

from dataclasses import dataclass

def is_valid_format_path(file_path:str) -> str:
    if file_path.split(".")[-1] in ['csv']:
        return True
    else:
        Exception(f"Invalid file format: {file_path}")
        return False

def upsert_into(df:DataFrame,table_name:str,keys:list) -> DataFrame:
    delta_obj = DeltaTable.forName(spark,table_name)
    return (
        delta_obj.alias("t").merge(
            df.alias("s")," AND ".join([f"t.{key} = s.{key}" for key in keys])
            )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
        )
    
    
@dataclass
class RegisterConfig:
    pipeline_name: str
    file_path: str
    header: str
    delimiter: str
    table_name: str
    schema_detail: str
    keys: 
    write_mode: str
    