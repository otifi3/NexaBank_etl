from file_monitor.file_monitor import FileMonitor
from pipeline.pipeline import Pipeline
from pipeline.logger.logger import Logger 

import warnings

# Suppress all warnings globally
warnings.filterwarnings("ignore")

def main():
    # Initialize logger
    logger = Logger('./logs/etl.log')  # Ensure you have a log file to capture logs

    # Instantiate the pipeline with the logger
    pipeline = Pipeline(logger)

    # Create an instance of the FileMonitor with the pipeline and the directory path to monitor
    file_monitor = FileMonitor(pipeline, base_dir="data/incomming_data")

    print("Starting file monitor...")
    # Start the file monitor to continuously check for new files and process them
    file_monitor.start()

if __name__ == "__main__":
    main()
