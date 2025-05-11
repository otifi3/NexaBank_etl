import pandas as pd

class TXTExtractor:
    def __init__(self):
        """
        Initializes the TXTExtractor with the path to the TXT file.

        :param file_path: Path to the TXT file.
        """

    def extract(self, sep: str, file_path: str) -> pd.DataFrame:
        """
        Extracts data from a TXT file and returns it as a pandas DataFrame.
        """
        # Read the TXT file into a DataFrame
        df = pd.read_csv(file_path, sep=sep)
        
        # Return the DataFrame
        return df