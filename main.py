from file_monitor.file_monitor import FileMonitor
from pipeline.pipeline import Pipeline  

def main():

    # Instantiate your pipeline
    pipeline = Pipeline()

    # Create an instance of the FileMonitor with the pipeline and directory path
    file_monitor = FileMonitor(pipeline, base_dir="/home/hadoop/data/incomming_data")

    # Start the file monitor to continuously check for new files and process them
    file_monitor.start()

if __name__ == "__main__":
    main()