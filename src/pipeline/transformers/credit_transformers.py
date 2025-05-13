import pandas as pd
from .transformer import Transformer


class CreditTransformers(Transformer):
    def __init__(self, logger):
        self.logger = logger
        self.file = 'credit_card'

    def transform(self, df) -> pd.DataFrame:
        """
        Transform the credit data.
        """
        try:
            df = self.add_fully_paid_column(df)
            df = self.add_debt_column(df)
            df = self.add_late_days_column(df)
            df = self.add_fine_column(df)
            df = self.add_total_amount_column(df)
            df = self.add_quality(df)
            df = self.conver_to_date(df, ['payment_date', 'partition_date'])
            
            return df
        except Exception as e:
            self.logger.log('error', f'Error during transformation: {self.file}')
            raise Exception(f"{self.file} Transformation Failed")

    def add_fully_paid_column(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add a new Boolean column 'fully_paid', set to True if the customer has paid the full bill amount, 
        and False otherwise.
        """
        df['fully_paid'] = df['amount_due'] == df['amount_paid']
        return df

    def add_debt_column(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add a new Integer column 'debt', representing the remaining due amount after payment.
        """
        df['debt'] = df['amount_due'] - df['amount_paid']
        return df

    def add_late_days_column(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add a new Integer column 'late_days', representing the number of days between the bill’s due date 
        (always the 1st of each month) and the actual payment date.
        """
        # Set the bill due date as the 1st of each month
        df['payment_date'] = pd.to_datetime(df['payment_date'])
        df['due_date'] = pd.to_datetime(df['month'] + '-01')  # Assuming 'month' format is 'YYYY-MM'
        df['late_days'] = (df['payment_date'] - df['due_date']).dt.days
        return df

    def add_fine_column(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add a new Float column 'fine', representing the fine charged to customers for late payments, 
        calculated as: fine = late_days * 5.15
        """
        df['fine'] = df['late_days'] * 5.15
        return df

    def add_total_amount_column(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add a new Float column 'total_amount', calculated as: total_amount = amount_due + fine
        """
        df['total_amount'] = df['amount_due'] + df['fine']
        return df
