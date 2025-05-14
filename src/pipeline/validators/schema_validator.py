import pandas as pd
import json

class SchemaValidator:
    def __init__(self, logger, schema_file: str):
        """
        Initialize the SchemaValidator by reading schemas from a JSON file.
        """
        with open(schema_file, 'r') as file:
            self.schemas = json.load(file)
        self.logger = logger
        self.file = None
        self.df = None

    def get_schema(self, file: str) -> dict:
        """
        Get the schema for a given file.
        """
        return self.schemas.get(file)

    def validate(self, df: pd.DataFrame, file: str) -> None:
        """
        Validate the schema of the DataFrame.
        If the schema is not valid, an error is logged and an exception is raised.
        """
        schema = self.get_schema(file)

        if not schema:
            error_message = f"No schema found for {file}."
            self.logger.log('error', error_message)
            raise ValueError(error_message)

        self.file = file
        self.df = df

        # Validate columns and their types
        for column, dtype in schema.items():
            if column not in df.columns:
                error_message = f"Missing column: {column} in {self.file}"
                self.logger.log('error', error_message)
                raise ValueError(error_message)

            # Get the actual dtype of the column
            actual_dtype = df[column].dtype

            if dtype == 'str' and actual_dtype != 'object':
                error_message = f"Column {column} is expected to be a string, but found {actual_dtype} in {self.file}."
                self.logger.log('error', error_message)
                raise ValueError(error_message)

            elif dtype == 'datetime':
                if not pd.api.types.is_datetime64_any_dtype(actual_dtype):
                    try:
                        df[column] = pd.to_datetime(df[column], errors='raise')
                    except Exception as e:
                        error_message = f"Error converting column {column} to datetime: {e} in {self.file}"
                        self.logger.log('error', error_message)
                        raise ValueError(error_message)

            elif dtype == 'int' and not pd.api.types.is_integer_dtype(actual_dtype):
                error_message = f"Column {column} is expected to be an integer, but found {actual_dtype} in {self.file}."
                self.logger.log('error', error_message)
                raise ValueError(error_message)

            elif dtype == 'float' and not pd.api.types.is_float_dtype(actual_dtype):
                error_message = f"Column {column} is expected to be a float, but found {actual_dtype} in {self.file}."
                self.logger.log('error', error_message)
                raise ValueError(error_message)

        self.logger.log('info', f"{self.file} schema validation passed.")
