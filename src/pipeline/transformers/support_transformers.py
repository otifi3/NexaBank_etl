import pandas as pd
from .transformer import Transformer

class SupportTransformers(Transformer):
    def __init__(self, logger):
        self.logger = logger
        self.file = 'support_data'

    def transform(self, df) -> pd.DataFrame:
        """
        Apply transformations to the Support data.
        """
        try:
            # df = self.convert_complaint_date(df)
            df = self.calculate_age(df, 'complaint_date')
            df = self.add_quality(df)
            df = self.conver_to_date(df, ['complaint_date', 'partition_date'])
            
            
            return df
        except Exception as e:
            self.logger.log('error', f'Error during transformation: {self.file}')
            raise Exception(f"{self.file} Transformation Failed")

    # def convert_complaint_date(self, df: pd.DataFrame) -> pd.DataFrame:
    #     """
    #     Convert 'complaint_date' column to datetime format.
    #     """
    #     df['complaint_date'] = pd.to_datetime(df['complaint_date'], format='%Y-%m-%d', errors='coerce').dt.date
    #     return df


