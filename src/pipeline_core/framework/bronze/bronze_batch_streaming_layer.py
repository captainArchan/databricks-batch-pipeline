from abc import ABC, abstractmethod


class BronzeDataPipeline(ABC):
    def run_bronze_pipeline():
        """"""
        read_data()


    @abstractmethod
    def read_data(self):
        """"""
        pass

