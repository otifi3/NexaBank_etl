import pandas as pd
import json

class JSONExtractor:
    def __init__(self, logger):
        """
        Initializes the JSONExtractor with the path to the JSON file.

        :param file_path: Path to the JSON file.
        """
        self.logger = logger

    def extract(self, file_path: str) -> pd.DataFrame:
        """
        Extracts data from a JSON file and returns it as a pandas DataFrame.
        """
        try:
            df = pd.read_json(file_path)
            return df
        except:
            self.logger.log('error', f'Wrong file path {file_path}')
            raise Exception(f"PipeLine Failed with {file_path}")

