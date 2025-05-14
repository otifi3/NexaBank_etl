import pandas as pd
from .transformer import Transformer
from pipeline.encryptors.encryptor import Encryptor

class LoanTransformers(Transformer):
    def __init__(self, logger, english_path: str):
        self.logger = logger
        self.Encryptor = Encryptor(english_path)
        self.file = 'loan_data'

    def transform(self, df) -> pd.DataFrame:
        """
        Transform the loan data.
        """
        try:
            df = self.calculate_age(df, 'utilization_date')
            df = self.calculate_total_cost(df)
            df = self.encrypt_loan_reason(df)
            df = self.add_quality(df)
            df = self.conver_to_date(df, ['utilization_date', 'partition_date'])
            
            return df
        except Exception as e:
            self.logger.log('error', f'Error during transformation: {self.file}')
            raise Exception(f"{self.file} Transformation Failed")
    
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

