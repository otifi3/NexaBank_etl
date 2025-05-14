import os
import time
import threading
from datetime import datetime
from queue import Queue

class FileMonitor:
    def __init__(self, pipeline, base_dir, clear_interval=3600):
        """
        Initialize the FileMonitor with a pipeline and the base directory to monitor.
        :param pipeline: The pipeline to process files.
        :param base_dir: The directory to monitor for new files.
        :param clear_interval: The time interval (in seconds) to clear the processed files set (default is 1 hour).
        """
        self.pipeline = pipeline
        self.base_dir = base_dir
        self.file_queue = Queue()  # Queue to hold the file paths for processing
        self.processed_files = set()  # Set to keep track of processed files
        self.clear_interval = clear_interval  # Interval to clear the processed files set
        self.last_clear_time = time.time()  # Keep track of when the set was last cleared
        self.next_target_hour = -1  

    def start(self):
        """
        Start monitoring the file system. Once a new file is detected in the partitioned hourly folder,
        run the pipeline with the file.
        """
        monitor_thread = threading.Thread(target=self.monitor_files)
        pipeline_thread = threading.Thread(target=self.process_files)

        monitor_thread.daemon = True
        pipeline_thread.daemon = True

        monitor_thread.start()
        pipeline_thread.start()

        monitor_thread.join()
        pipeline_thread.join()

    def monitor_files(self):
        """
        Continuously monitor the directory for new files and add them to the file queue.
        """
        while True:
            self.check_and_clear_processed_files()  
            file = self.detect_new_file()
        
            if file and file not in self.processed_files:
                self.file_queue.put(file)  
                self.processed_files.add(file)  
            else:
                time.sleep(1)  
    def process_files(self):
        """
        Continuously retrieve file paths from the queue, process the file with the pipeline,
        and remove it from the queue after processing.
        """
        while True:
            if not self.file_queue.empty():
                file = self.file_queue.get()  # Retrieve the next file from the queue
                self.pipeline.run(file)  # Process the file using the pipeline
                self.file_queue.task_done()  # Mark the task as done
                self.remove_file(file)  # Remove the file after processing

            else:
                time.sleep(1)  

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

        dir_path = os.path.join(self.base_dir, f"{year}-{month:02d}-{day:02d}", f"{hour:02d}")

        if os.path.exists(dir_path):
            files = self.list_files(dir_path)
            for file in files:
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


    def remove_file(self, file):
        """
        Remove the file from the directory to prevent it from being processed again.
        """
        if os.path.exists(file):
            os.remove(file)


    # def check_and_clear_processed_files(self):
    #     """
    #     Check if the time has passed for the interval to clear the processed files set.
    #     Clears the set if the interval is reached.
    #     """
    #     current_time = time.time()
    #     if current_time - self.last_clear_time >= self.clear_interval:
    #         self.processed_files.clear()  
    #         self.last_clear_time = current_time 

    def check_and_clear_processed_files(self):
        """
        Check if the time has reached the next full hour to clear the processed files set.
        Clears the set when the current time hits the next full hour (e.g., 5:00, 6:00, etc.).
        """
        current_hour = datetime.now().hour  
        current_minute = datetime.now().minute  
        if current_hour == self.next_target_hour:  
            self.processed_files.clear()  
        if current_minute == 0:
            self.next_target_hour = current_hour + 1 if current_hour < 23 else 0 
        else:
            self.next_target_hour = current_hour + 1 if current_hour < 23 else 0  

    
