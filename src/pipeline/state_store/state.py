import json
import os
import pandas as pd
from datetime import datetime
import threading


class StateStore:
    def __init__(self, logger, directory):
        """
        Initializes the state store for any given table dynamically.
        
        :param directory: Directory where the state store files are located.
        """
        self.directory = directory
        self.logger = logger



    def load_state(self, table_name, column_name) -> list:
        """
        Loads the current state (e.g., bill_id or complaint_date) from the JSON file.
        Returns the current value (list or single value), or None if the file doesn't exist.
        
        :param table_name: The name of the table for which the state is to be retrieved.
        :param column_name: The name of the column for which the state is to be retrieved.
        """
        file_path = f"{self.directory}/{table_name}.json"
        
        if not os.path.exists(file_path):
            return None  
        
        with open(file_path, "r") as file:
            data = json.load(file)
            return data.get(column_name, None) 

    def save_state(self, table_name, column_name, value) -> None:
        """
        Saves the current state (e.g., bill_id or complaint_date) to the JSON file.
        
        :param table_name: The name of the table to store the state for.
        :param column_name: The name of the column to store the state for.
        :param value: The value to be stored (e.g., list of bill_id or complaint_date).
        """
        file_path = f"{self.directory}/{table_name}.json"

        if os.path.exists(file_path):
            with open(file_path, "r") as file:
                data = json.load(file)
        else:
            data = {}

        data[column_name] = value
        
        with open(file_path, "w") as file:
            json.dump(data, file, indent=4)

    def update_or_add(self, table_name, column_name, value) -> None:
        """
        Update or add the value to the state store.
        If the state is a list (e.g., bill_id), append the values.
        If the state is a single value (e.g., complaint_date), update it.
        
        :param table_name: The name of the table for which the value should be added or updated.
        :param column_name: The column name for which the value should be added or updated.
        :param value: The new value(s) to update or add (e.g., bill_id or complaint_date).
        """
        if isinstance(value, list):
            new_values = value  
        else:
            new_values = [value] 

        current_state = self.load_state(table_name, column_name)  

        if isinstance(current_state, list):
            for val in new_values:
                if val not in current_state:
                    current_state.append(val)  
            self.save_state(table_name, column_name, current_state) 
        
        elif isinstance(current_state, str):
            for val in new_values:
                if current_state != val:
                    self.save_state(table_name, column_name, val)  
        elif current_state is None:
            self.save_state(table_name, column_name, new_values)
    
    def update_or_add_threaded(self, table_name, column_name, value):
        """
        Runs the `update_or_add` method in a separate thread to avoid blocking the main process.
        
        :param table_name: The name of the table for which the value should be added or updated.
        :param column_name: The column name for which the value should be added or updated.
        :param value: The new value(s) to update or add (e.g., bill_id or complaint_date).
        """
        threading.Thread(target=self.update_or_add, args=(table_name, column_name, value)).start()

    def filter(self, df, table_name, column_name):
        """
        Filters the DataFrame based on the state store value.
        If the state is a list (e.g., bill_id), exclude rows with those values.
        If the state is a single date (e.g., complaint_date), keep rows with dates greater than the stored date.
        
        :param df: The DataFrame to be filtered.
        :param table_name: The name of the table to retrieve the state for.
        :param column_name: The column name to filter based on the state value.
        
        :return: The filtered DataFrame.
        """
        current_state = self.load_state(table_name, column_name) 

        ## Log the current state

        if isinstance(current_state, list):
            df_filtered = df[~df[column_name].isin(current_state)]
            self.logger.log('info', f"Filtered {table_name} to => {df_filtered.shape[0]} rows.")
            
            # After filtering, update the state with the new list of values in the filtered DataFrame
            new_values = df_filtered[column_name].unique().tolist()
            self.update_or_add_threaded(table_name, column_name, new_values)  # Append new values to the state list

        elif isinstance(current_state, str):  
            df_filtered = df[df[column_name] > current_state]
            self.logger.log('info', f"Filtered {table_name} to => {df_filtered.shape[0]} rows.")
            
            # After filtering, update the state with the max date from the filtered DataFrame
            if not df_filtered.empty:
                max_date = df_filtered[column_name].max()
                self.update_or_add_threaded(table_name, column_name, max_date)
            else:
                self.logger.log('warning', f"{table_name} file is already processed.")
                raise ValueError(f"break pipeline for {table_name}.")
        
        else:
            df_filtered = df
            self.logger.log('warning', f"No state for {table_name}.")

        # If the filtered DataFrame is empty after applying the filter
        if df_filtered.empty:
            self.logger.log('warning', f"{table_name} file is already processed.")
            raise ValueError(f"Close pipeline for {table_name} becuse it it EMPTY...")
        
        return df_filtered


