from abc import ABC, abstractmethod


class BronzeDataPipeline(ABC):
    def run_bronze_pipeline(self):
        df = self.read_data()
        df = self.encrypt_df(df)
        self.write_data(df)

    @abstractmethod
    def read_data(self) -> DataFrame:
        pass
    
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

    @abstractmethod
    def write_data(self, df:Dataframe) -> None:
        pass
    




