import pandas as pd

class JSONExtractor:
    def __init__(self, file_path: str):
        """
        Initializes the JSONExtractor with the path to the JSON file.

        :param file_path: Path to the JSON file.
        """

    def extract(self) -> pd.DataFrame:
        """
        Extracts data from a JSON file and returns it as a pandas DataFrame.
        """