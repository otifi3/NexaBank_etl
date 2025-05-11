import pandas as pd
from .transformer import Transformer


class MoneyTransformers(Transformer):
    def __init__(self):
        pass

    def transform(self, df) -> pd.DataFrame:
        """
        Apply transformations to the transaction data.
        """
        df = self.add_cost_column(df)
        df = self.add_total_amount_column(df)
        return df

    def add_cost_column(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add a new float column 'cost' representing the cost of the transaction.
        The cost is 50 cents + 0.1% of the transaction amount.
        """
        df['cost'] = 0.50 + (df['transaction_amount'] * 0.001)
        return df

    def add_total_amount_column(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add a new float column 'total_amount', calculated as: 
        total_amount = transaction_amount + cost
        """
        df['total_amount'] = df['transaction_amount'] + df['cost']
        return df
