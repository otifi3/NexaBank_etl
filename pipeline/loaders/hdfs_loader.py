import subprocess
import os

class HDFSLoader:
    def __init__(self, hdfs_path):
        """
        Initializes the HDFSLoader with the path to the HDFS location.

        :param hdfs_path: Path to the HDFS location.
        """
        self.hdfs_path = hdfs_path

    def save_as_parquet(self, df, local_path) -> None:
        """
        Saves the DataFrame as a Parquet file locally.

        :param df: The DataFrame to be saved.
        :param local_path: The local path where the Parquet file will be saved.
        """
        df.to_parquet(local_path, index=False)

    def load(self, df, local_path='tmp/') -> None:
        """
        Loads the DataFrame to HDFS after saving it locally as a Parquet file.

        :param df: DataFrame to be loaded.
        :param local_path: The local path where the Parquet file will be saved before uploading to HDFS.
        """
        self.save_as_parquet(df, local_path)

        try:
            cmd = ["hdfs", "dfs", "-put", local_path, self.hdfs_path]

            subprocess.run(cmd, check=True)

            print(f"File successfully uploaded to HDFS at {self.hdfs_path}")

        except subprocess.CalledProcessError as e:
            print(f"Failed to upload file to HDFS: {e}")
        finally:
            if os.path.exists(local_path):
                os.remove(local_path)
                print(f"Local file {local_path} removed after upload.")

