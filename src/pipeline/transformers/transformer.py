import pandas as pd
from datetime import datetime

class Transformer():
    def __init__(self):
        pass
        
    def add_quality(self, df) -> pd.DataFrame:
        """
        add_quality function to add quality column to the dataframe
        """
        df['processing_time'] = datetime.now().strftime('%H:%M:%S')  # Format as HH:MM:SS

        df['partition_date'] = datetime.now().strftime('%Y-%m-%d')  # Format as YYYY-MM-DD

        df['partition_hour'] = datetime.now().hour  
    
        return df
    
    def conver_to_date(self, df, columns) -> pd.DataFrame:
        """
        convert_to_datetime function to convert columns to datetime
        """
        for column in columns:
            # df[column] =  df[column].astype(str)
            df[column] = pd.to_datetime(df[column], format='%Y-%m-%d', errors='coerce').dt.date
        return df
    
    def calculate_age(self, df: pd.DataFrame, date_column: str ) -> pd.DataFrame:
        """
        Calculate the age (days since specific date).
        """
        df['age'] = (pd.to_datetime('today') - pd.to_datetime(df[date_column])).dt.days
        return df