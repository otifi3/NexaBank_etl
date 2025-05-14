import json
import os

class StateStore:
    def __init__(self, directory):
        """
        Initializes the state store for any given table dynamically.
        
        :param directory: Directory where the state store files are located.
        """
        self.directory = directory

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


