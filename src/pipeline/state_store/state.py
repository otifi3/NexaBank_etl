import os
import pandas as pd

class StateStore:
    """
    A class to manage loading, saving, updating, and filtering state information for 
    tables and columns using parquet files as persistent storage.

    The state can be either a scalar string or a list of strings representing processed values,
    which is used to filter new incoming data.

    Attributes:
        directory (str): Directory path where state parquet files are stored.
        logger: Logger instance for logging info and warnings.
        _state (str or list): Current loaded state (single string or list of strings).
        _current_table (str): The currently loaded table name.
        _current_column (str): The currently loaded column name.
    """

    def __init__(self, logger, directory):
        """
        Initialize the StateStore instance.

        Args:
            logger: Logger instance for logging.
            directory (str): Directory to save/load parquet state files.
        """
        self.directory = directory
        self.logger = logger
        self._state = None           # current loaded state (list or scalar)
        self._current_table = None
        self._current_column = None

    def _get_file_path(self, table_name) -> str:
        """
        Generate the file path for the parquet state file for the given table.

        Args:
            table_name (str): The table name.

        Returns:
            str: Full file path for the parquet file storing the state.
        """
        filename = f"{table_name}.parquet"
        return os.path.join(self.directory, filename)
    
    def load_state(self, table_name, column_name) -> None:
        """
        Load the state from a parquet file for the specified table and column.

        If the file does not exist or is empty, sets the state to None.
        If the specified column is missing, logs a warning and sets state to None.
        Converts all loaded state values to strings.

        Args:
            table_name (str): The table name.
            column_name (str): The column name to load state for.
        """
        path = self._get_file_path(table_name)
        if os.path.exists(path):
            df = pd.read_parquet(path)
            if df.empty or column_name not in df.columns:
                self.logger.log('warning', f"State file {path} is empty or missing column '{column_name}'.")
                self._state = None
            else:
                if df.shape[0] > 1:
                    self._state = df[column_name].astype(str).tolist()
                else:
                    self._state = str(df[column_name].iloc[0])
                self.logger.log('info', f"Loaded state for {table_name}.{column_name} from {path}")
        else:
            self.logger.log('info', f"No existing state file for {table_name}.{column_name}, starting empty")
            self._state = None

        self._current_table = table_name
        self._current_column = column_name

    def flush(self) -> None:
        """
        Save the current in-memory state to a parquet file for the current table and column.

        If no current table or column is loaded, or state is None, logs a warning and does nothing.
        Saves lists as multiple rows; scalar as a single-row dataframe.
        """
        if self._current_table is None or self._current_column is None:
            self.logger.log('warning', "No current table/column loaded, nothing to save.")
            return

        if self._state is None:
            self.logger.log('warning', f"No state to save for {self._current_table}.{self._current_column}")
            return

        path = self._get_file_path(self._current_table)
        if isinstance(self._state, list):
            df = pd.DataFrame({self._current_column: self._state})
        else:
            df = pd.DataFrame({self._current_column: [self._state]})
        df.to_parquet(path, index=False)
        self.logger.log('info', f"Saved state for {self._current_table} to {path}")

    def update_or_add(self, new_value) -> None:
        """
        Update the in-memory state with new_value.

        - If current state is None, sets it to new_value.
        - If state is a list, merges new_value(s) into the list (supports list or scalar).
        - If state is a scalar string, updates it if new_value is lexicographically greater.

        Args:
            new_value (str or list): New value(s) to add to the state.
        """
        if self._state is None:
            self._state = new_value
            return

        if isinstance(self._state, list):
            combined = set(self._state)
            if isinstance(new_value, list):
                combined.update(new_value)
            else:
                combined.add(new_value)
            self._state = list(combined)  # Ensure unique values (no duplicates)
        else:
            if isinstance(new_value, list):
                self._state = max(new_value)
            elif new_value > self._state:
                self._state = new_value

    def filter(self, df, table_name, column_name) -> pd.DataFrame:
        """
        Filter the DataFrame based on the current state for the specified table and column.

        Logic:
        - If state is a list of strings, exclude rows where column value is in that list.
        - If state is a scalar string, keep only rows where column value is lexicographically greater than the state.

        After filtering, update the in-memory state with new values found in the filtered DataFrame.

        Raises:
            ValueError: If filtering results in an empty DataFrame.

        Args:
            df (pd.DataFrame): The DataFrame to filter.
            table_name (str): The table name.
            column_name (str): The column name to filter on.

        Returns:
            pd.DataFrame: The filtered DataFrame.
        """
        self.load_state(table_name, column_name)

        if self._state is None:
            self.logger.log('warning', f"No state loaded for {table_name}.{column_name}, returning unfiltered DataFrame")
            return df

        if isinstance(self._state, list):
            # Exclude rows where the column value is in the state list
            filtered_df = df.loc[~df[column_name].isin(self._state)]
            self.logger.log('info', f"Filtered {table_name}.{column_name} (list state), remaining rows => {filtered_df.shape[0]}")
            new_vals = filtered_df[column_name].unique().tolist()
            if new_vals:
                self.update_or_add(new_vals)
        else:
            val = self._state
            # Exclude rows where the column value is less than or equal to the state
            filtered_df = df.loc[df[column_name] > val]
            self.logger.log('info', f"Filtered {table_name}.{column_name} (scalar state), remaining rows => {filtered_df.shape[0]}")
            if not filtered_df.empty:
                max_new = filtered_df[column_name].max()  # Get the max value in the filtered rows
                self.update_or_add(max_new)

        # If filtering results in an empty DataFrame, raise an error
        if filtered_df.empty:
            self.logger.log('warning', f"Filtering on {table_name}.{column_name} resulted in empty DataFrame")
            raise ValueError(f"No data left after filtering on {table_name}.{column_name}")

        return filtered_df
