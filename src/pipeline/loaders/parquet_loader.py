import os
import pandas as pd

class ParquetLoader:
    def __init__(self, logger, output_dir):
        """
        Initialize the ParquetWriter with the directory to save Parquet files.
        
        :param logger: Logger instance to log messages.
        :param output_dir: Directory where Parquet files will be saved.
        """
        self.logger = logger
        self.output_dir = output_dir

    def load(self, df, file_name):
        """
        Writes a DataFrame to a Parquet file in the specified directory.
        
        :param df: DataFrame to be written to Parquet.
        :param file_name: Name of the Parquet file (without extension).
        """
        # Construct the full file path
        file_path = os.path.join(self.output_dir, f"{file_name}.parquet")
        
        try:
            # Write the DataFrame to the Parquet file
            df.to_parquet(file_path, engine='pyarrow')  # You can use 'fastparquet' if you prefer
            
            # Log success message
            self.logger.log('info', f"Data successfully written to {file_path}")
        except Exception as e:
            # Log the error message
            self.logger.log('error', f"Error writing to Parquet: {e}")
            raise Exception(f"Failed to write DataFrame to Parquet file {file_path}: {e}")
