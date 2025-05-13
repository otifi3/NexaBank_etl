import pandas as pd

class TXTExtractor:
    def __init__(self, logger):
        """
        Initializes the TXTExtractor with the path to the TXT file.

        :param file_path: Path to the TXT file.
        """
        self.logger = logger

    def extract(self, file_path: str, sep: str) -> pd.DataFrame:
        """
        Extracts data from a TXT file and returns it as a pandas DataFrame.
        """
        try:
            df = pd.read_csv(file_path, sep=sep)
            return df
        except:
            self.logger.log('error', f'Wrong file path {file_path}')
            raise Exception(f"PipeLine Failed with {file_path}")
