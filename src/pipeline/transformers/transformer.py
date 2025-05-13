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