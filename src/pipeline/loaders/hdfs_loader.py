import subprocess
import os

class HDFSLoader:
    def __init__(self, logger):
        """
        Initializes the HDFSLoader with the logger and HDFS path.

        :param logger: Logger instance to log messages.
        """
        self.logger = logger

    def load(self, hdfspath, local_path, timeout = 5) -> None:
        """
        Loads the DataFrame to HDFS after saving it locally as a Parquet file.

        :param hdfspath: HDFS destination path.
        :param local_path: The local path where the Parquet file will be saved before uploading to HDFS.
        """
        try:
            # Command to upload the file to HDFS
            cmd = ["hdfs", "dfs", "-put", local_path, hdfspath]

            subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout)

            # Run the HDFS upload command
            # subprocess.run(cmd, check=True)

            # Log success message
            self.logger.log('info', f"File successfully uploaded to HDFS at {hdfspath}")

        except Exception as e:
            # Log the error message
            self.logger.log('error', f"Failed to upload file to HDFS: {e}")
            
            if os.path.exists(local_path):
                os.remove(local_path)
                self.logger.log('error', f"Local file {local_path} removed after failed upload.")
            raise Exception(f"Failed to upload file to HDFS: {e}")
        
        finally:
            if os.path.exists(local_path):
                os.remove(local_path)
                self.logger.log('info', f"Local file {local_path} removed after upload.")
