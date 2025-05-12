import os
import time
from datetime import datetime

class FileMonitor:
    def __init__(self, pipeline, base_dir):
        """
        Initialize the FileMonitor with a pipeline and the base directory to monitor.
        """
        self.pipeline = pipeline  
        self.base_dir = base_dir  
        self.last_check_time = time.time() 

    def start(self):
        """
        Start monitoring the file system. Once a new file is detected in the partitioned hourly folder,
        run the pipeline with the file.
        """
        while True:
            file = self.detect_new_file()
            if file:
                self.pipeline.run(file)  
                self.delete_file(file)  
            else:
                time.sleep(10)  

    def detect_new_file(self):
        """
        Detect new files in the partitioned directory structure based on modification time.
        Returns the new file path if a new file is detected.
        """
        current_time = datetime.now()
        day = current_time.day
        month = current_time.month
        year = current_time.year
        hour = current_time.hour

        # Construct directory path for the current date and time
        dir_path = os.path.join(self.base_dir, f"{year}-{month:02d}-{day:02d}", f"{hour:02d}")

        if os.path.exists(dir_path):
            files = self.list_files(dir_path)
            for file in files:
                if self.is_recently_modified(file):
                    return file
        return None

    def list_files(self, dir_path):
        """
        List files in the directory. This method returns a list of file paths.
        """
        try:
            files = [os.path.join(dir_path, file) for file in os.listdir(dir_path) if os.path.isfile(os.path.join(dir_path, file))]
            return files
        except FileNotFoundError:
            return []  

    def is_recently_modified(self, file):
        """
        Check if a file was modified after the last check time.
        Returns True if the file was modified after the last check.
        """
        file_mtime = os.path.getmtime(file)
        return file_mtime > self.last_check_time

    def update_last_check_time(self):
        """
        Update the timestamp of the last check time to the current time.
        """
        self.last_check_time = time.time()