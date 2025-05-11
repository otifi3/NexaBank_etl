import pandas as pd

class CSVExtractor:
    def __init__(self, file_path: str):
        """
        Initializes the CSVExtractor with the path to the CSV file.

        :param file_path: Path to the CSV file.
        """

    def extract(self) -> pd.DataFrame:
        """
        Extracts data from a CSV file and returns it as a pandas DataFrame.
        """
        # Read the CSV file into a DataFrame
        df = pd.read_csv(self.file_path)
        
        # Return the DataFrame
        return df