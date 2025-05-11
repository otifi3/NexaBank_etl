import pandas as pd
from transformer import Transformer

class SupportTransformers(Transformer):
    def __init__(self):
        pass
    
    def transform(self, df) -> pd.DataFrame:
        """
        Apply transformations to the Support.
        """
        df = self.convert_complaint_date(df)
        df = self.calculate_age(df)
        return df

    def convert_complaint_date(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Convert 'complaint_date' column to datetime format.
        """
        df['complaint_date'] = pd.to_datetime(df['complaint_date'])
        return df

    def calculate_age(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate the age (days since the complaint date).
        """
        df['age'] = (pd.to_datetime('today') - df['complaint_date']).dt.days
        return df
