import pandas as pd
from pipeline.transformers.transformer import Transformer
from pipeline.encryptors.encryptor import Encryptor

class LoanTransformers(Transformer):
    def __init__(self, english_path: str):
        self.Encryptor = Encryptor(english_path)

    def transform(self, df) -> pd.DataFrame:
        df = self.convert_utilization_date(df)
        df = self.calculate_total_cost(df)
        df = self.encrypt_loan_reason(df) 
        df = self.add_quality(df)

        return df
    def convert_utilization_date(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Convert 'utilization_date' column to the number of days since the date
        """
        df['utilization_date'] = pd.to_datetime(df['utilization_date'])
        df['utilization_date'] = (pd.to_datetime('today') - df['utilization_date']).dt.days
        return df

    def calculate_total_cost(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate the 'total_cost' column based on the 'amount_utilized'
        """
        df['total_cost'] = df['amount_utilized'] * 0.20 + 1000
        return df
    def encrypt_loan_reason(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Encrypt the 'loan_reason' column using the Encryptor's encrypt method.
        """
        df = self.Encryptor.encrypt(df, 'loan_reason') 
        return df

