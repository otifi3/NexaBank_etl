import pandas as pd
from .transformer import Transformer


class CustomerTransformers(Transformer):
    def __init__(self, logger):
        self.logger = logger
        self.file = 'customer_data'

    def transform(self, df) -> pd.DataFrame:
        """
        Transform the customer data.
        """
        try:
            df = self.add_tenure(df)
            df = self.categorize_customer_segment(df)
            df = self.add_quality(df)
            return df
        except Exception as e:
            self.logger.log('error', f'Error during transformation: {self.file}')
            raise Exception(f"{self.file} Transformation Failed")

    def add_tenure(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add a new column 'tenure' which is the difference in years between the current date
        and the account open date.
        """
        df['account_open_date'] = pd.to_datetime(df['account_open_date'])
        df['tenure'] = (pd.to_datetime('today').year - df['account_open_date'].dt.year)
        return df

    def categorize_customer_segment(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Categorize customers into 'Loyal', 'Newcomer', or 'Normal' based on their tenure.
        """
        df['customer_segment'] = df['tenure'].apply(self.categorize_customer)
        return df

    def categorize_customer(self, tenure: int) -> str:
        """
        Classify customers based on their tenure.
        """
        if tenure > 5:
            return 'Loyal'
        elif tenure < 1:
            return 'Newcomer'
        else:
            return 'Normal'
