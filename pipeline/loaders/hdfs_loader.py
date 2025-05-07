import pandas as pd

class HDFSLoader:
    def __init__(self, hdfs_path):
        """
        Initializes the HDFSLoader with the path to the HDFS location.

        :param hdfs_path: Path to the HDFS location.
        """
        self.hdfs_path = hdfs_path

    def load(self, df) -> None:
        """
        Loads the DataFrame to HDFS.

        :param df: DataFrame to be loaded.
        """
    
        pass
        