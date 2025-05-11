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