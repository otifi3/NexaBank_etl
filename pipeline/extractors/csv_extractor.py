import pandas as pd

class CSVExtractor:
    def __init__(self, logger):

        """
        Initializes the CSVExtractor with the path to the CSV file.

        :param file_path: Path to the CSV file.
        """
        self.logger = logger
    def extract(self, file_path: str) -> pd.DataFrame:
        """
        Extracts data from a CSV file and returns it as a pandas DataFrame.
        """
        # Read the CSV file into a DataFrame
        try:
            df = pd.read_csv(file_path) 
            return df
        except:
            self.logger.log('error', f'Wrong file path {file_path}')
            raise Exception(f"PipeLine Failed with {file_path}")
