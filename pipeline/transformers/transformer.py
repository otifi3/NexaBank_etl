import pandas as pd
from abc import ABC, abstractmethod
class Transformer(ABC):
    def __init__(self):
        pass
    @abstractmethod
    def transform(self, df) -> pd.DataFrame:
        """
        Abstract method to transform the dataframe
        """
        pass

    @staticmethod    
    def add_quality(df) -> pd.DataFrame:
        """
        add_quality function to add quality column to the dataframe
        """
    
        return df