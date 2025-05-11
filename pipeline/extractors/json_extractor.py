import pandas as pd
import json

class JSONExtractor:
    def __init__(self):
        """
        Initializes the JSONExtractor with the path to the JSON file.

        :param file_path: Path to the JSON file.
        """

    def extract(self, file_path: str) -> pd.DataFrame:
        """
        Extracts data from a JSON file and returns it as a pandas DataFrame.
        """
        with open(file_path, 'r') as file:
            data = json.load(file)

        # Normalize the JSON data into a flat table
        df = pd.json_normalize(data)

        return df
