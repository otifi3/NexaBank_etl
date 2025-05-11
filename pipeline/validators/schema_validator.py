import pandas as pd
import json

class SchemaValidator:
    def __init__(self, schema_file: str):
        """
        Initialize the SchemaValidator by reading schemas from a JSON file.
        """
        with open(schema_file, 'r') as file:
            self.schemas = json.load(file)
        self.file = None
        self.df = None

    def get_schema(self, file: str) -> dict:
        """
        Get the schema for a given file.
        """
        return self.schemas.get(file)

    def validate(self, df: pd.DataFrame, file: str) -> bool: 
        """
        Validate the schema of the DataFrame.
        """
        schema = self.get_schema(file)

        if not schema:
            print(f"No schema found for {file}.")
            return False

        self.file = file
        self.df = df

        # Validate columns and their types
        for column, dtype in schema.items():
            if column not in df.columns:
                return False

            # Get the actual dtype of the column
            actual_dtype = df[column].dtype
            if dtype == 'datetime' and not pd.api.types.is_datetime64_any_dtype(actual_dtype):
                return False
            elif dtype == 'str' and not pd.api.types.is_string_dtype(actual_dtype):
                return False
            elif dtype == 'int' and not pd.api.types.is_integer_dtype(actual_dtype):
                return False
            elif dtype == 'float' and not pd.api.types.is_float_dtype(actual_dtype):
                return False

        return True
