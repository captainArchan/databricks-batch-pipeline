from abc import ABC, abstractmethod


class BronzeDataPipeline(ABC):
    def run_bronze_pipeline(self):
        df = self.read_data()
        self.write_data(df)

    @abstractmethod
    def read_data(self) -> DataFrame:
        pass
    

    @abstractmethod
    def write_data(self, df:Dataframe) -> None:
        pass
    




