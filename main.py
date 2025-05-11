from file_monitor.file_monitor import FileMonitor
from pipeline.pipeline import Pipeline  

from pipeline.encryptors.encryptor import Encryptor
from pipeline.logger.logger import Logger
from pipeline.notifier.email_notifier import EmailNotifier
from pipeline.validators.schema_validator import SchemaValidator

from pipeline.extractors.csv_extractor import CSVExtractor
from pipeline.extractors.json_extractor import JSONExtractor
from pipeline.extractors.txt_extractor import TXTExtractor

from pipeline.transformers.transformer import Transformer
from pipeline.transformers.credit_transformers import CreditTransformers
from pipeline.transformers.customer_transformers import CustomerTransformers
from pipeline.transformers.Loans_transformers import LoanTransformers
from pipeline.transformers.support_transformers import SupportTransformers
from pipeline.transformers.money_transfers_transformers import MoneyTransformers

from pipeline.loaders.hdfs_loader import HDFSLoader

def main():
    
    
    # Instantiate your pipeline
    pipeline = Pipeline()

    # Create an instance of the FileMonitor with the pipeline and directory path
    file_monitor = FileMonitor(pipeline, base_dir="/home/otifi/ITI/python/etl/data")

    # Start the file monitor to continuously check for new files and process them
    file_monitor.start()

if __name__ == "__main__":
    main()