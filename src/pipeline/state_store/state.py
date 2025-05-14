import os
import pandas as pd

class StateStore:
    def __init__(self, logger, directory):
        self.directory = directory
        self.logger = logger
        self._state = None           # current loaded state (list or scalar)
        self._current_table = None
        self._current_column = None

    def _get_file_path(self, table_name):
        filename = f"{table_name}.parquet"
        return os.path.join(self.directory, filename)
    
    def load_state(self, table_name, column_name):
        path = self._get_file_path(table_name)
        if os.path.exists(path):
            df = pd.read_parquet(path)
            if df.empty or column_name not in df.columns:
                self.logger.log('warning', f"State file {path} is empty or missing column '{column_name}'.")
                self._state = None
            else:
                # If multiple rows => load all as list
                if len(df) > 1:
                    self._state = df[column_name].tolist()
                else:
                    self._state = df[column_name].iloc[0]
                self.logger.log('info', f"Loaded state for {table_name}.{column_name} from {path}")
        else:
            self.logger.log('info', f"No existing state file for {table_name}.{column_name}, starting empty")
            self._state = None

        self._current_table = table_name
        self._current_column = column_name


    def flush(self):
        """Save current in-memory state to parquet file for the current table and column."""
        if self._current_table is None or self._current_column is None:
            self.logger.log('warning', "No current table/column loaded, nothing to save.")
            return

        if self._state is None:
            self.logger.log('warning', f"No state to save for {self._current_table}.{self._current_column}")
            return

        path = self._get_file_path(self._current_table)
        df = pd.DataFrame({self._current_column: [self._state]})
        df.to_parquet(path, index=False)
        self.logger.log('info', f"Saved state for {self._current_table} to {path}")

    def update_or_add(self, new_value):
        """
        Update in-memory state with new_value.
        Supports merging lists or updating scalar if greater.
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
            self._state = list(combined)
        else:
            # scalar case: update if new_value is greater
            if new_value > self._state:
                self._state = new_value

    def filter(self, df, table_name, column_name):
        """
        Load state for given table and column, then filter DataFrame based on it:
        - If state is list: exclude rows where df[column_name] in list.
        - If scalar: keep rows where df[column_name] > scalar.

        After filtering, update the in-memory state with the new values.

        Raises ValueError if filtering results in empty DataFrame.
        """
        self.load_state(table_name, column_name)

        print(self._state)

        if column_name not in df.columns:
            self.logger.log('warning', f"Column '{column_name}' not in DataFrame, returning unfiltered DataFrame")
            return df

        if self._state is None:
            self.logger.log('warning', f"No state loaded for {table_name}.{column_name}, returning unfiltered DataFrame")
            return df

        if isinstance(self._state, list):
            print(self._state)

            if len(self._state) == 0:
                self.logger.log('info', f"State list for {table_name}.{column_name} is empty, skipping exclusion filter.")
                filtered_df = df
            else:
                filtered_df = df[~df[column_name].isin(self._state)]
                self.logger.log('info', f"Filtered {table_name}.{column_name} by excluding {len(self._state)} known values.\n remaining rows: {filtered_df.shape[0]}")

            # Update in-memory state with new unique values from filtered DataFrame
            new_vals = filtered_df[column_name].unique().tolist()
            if new_vals:
                self.update_or_add(new_vals)

        else:
            filtered_df = df[df[column_name] > self._state]
            self.logger.log('info', f"Filtered {table_name}.{column_name} by keeping values > {self._state}, remaining rows: {filtered_df.shape[0]}")

            if not filtered_df.empty:
                max_new = filtered_df[column_name].max()
                self.update_or_add(max_new)

        if filtered_df.empty:
            self.logger.log('warning', f"Filtering on {table_name}.{column_name} resulted in empty DataFrame")
            raise ValueError(f"Close pipeline because no data left after filtering on {table_name}.{column_name}")
        print(self._state)

        return filtered_df

