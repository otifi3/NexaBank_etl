from dotenv import load_dotenv
import os
import shutil
from pipeline.extractors.csv_extractor import CSVExtractor
from pipeline.extractors.txt_extractor import TXTExtractor
from pipeline.extractors.json_extractor import JSONExtractor

from pipeline.transformers.credit_transformers import CreditTransformers
from pipeline.transformers.customer_transformers import CustomerTransformers
from pipeline.transformers.Loans_transformers import LoanTransformers
from pipeline.transformers.support_transformers import SupportTransformers
from pipeline.transformers.money_transfers_transformers import MoneyTransformers
from pipeline.loaders.parquet_loader import ParquetLoader 
from pipeline.loaders.hdfs_loader import HDFSLoader 

from pipeline.validators.schema_validator import SchemaValidator
from pipeline.notifier.email_notifier import EmailNotifier 
from pipeline.loaders.hdfs_loader import HDFSLoader 
from pipeline.logger.logger import Logger 
from pipeline.state_store.state import StateStore

class Pipeline:
    def __init__(self, logger: Logger):
        load_dotenv()  # Load environment variables from .env
        self.user = os.getenv("EMAIL_USER")
        self.password = os.getenv("EMAIL_PASSWORD")
        self.smtp_server = 'smtp.gmail.com'
        self.smtp_port = 587

        # Assign the logger
        self.logger = logger

        # Initialize extractors
        self.extractors = {
            "csv": CSVExtractor(logger),
            "txt": TXTExtractor(logger),
            "json": JSONExtractor(logger)
        }

        # Initialize transformers
        self.transformers = {
            "credit_cards_billing": CreditTransformers(logger),
            "customer_profiles": CustomerTransformers(logger),
            "support_tickets": SupportTransformers(logger),
            "loans": LoanTransformers(logger, '/home/hadoop/src/pipeline/support/english_words.txt'),
            "transactions": MoneyTransformers(logger)
        }

        # Initialize the schema validator and email notifier
        self.validator = SchemaValidator(logger, '/home/hadoop/src/pipeline/support/schemas.json')  
        self.notifier = EmailNotifier(self.smtp_server, self.smtp_port, self.user, self.password) 
        self.parquet_loader = ParquetLoader(logger, './tmp')
        self.hdfs_loader = HDFSLoader(logger) 

    def run(self, file):
        """
        Process the file using the appropriate extractor, transformer, validator, and loader.
        """
        

        # Extract file extension
        file_extension = file.split('.')[-1].lower()

        # Extract file type from the file name (without extension)
        file_type = file.split('/')[-1].rsplit('_', 1)[0]

        # start processing
        self.logger.log('info', f"Processing file: {file_type}")

        try:
            # Dynamically select the correct extractor based on the file extension
            extractor = self.extractors.get(file_extension)
            if not extractor:
                self.logger.log('error', f"Unsupported file type: {file_extension}")
                raise ValueError(f"Unsupported file type: {file_extension}")
            if file_extension == 'txt':
                # For TXT files, specify the delimiter
                df = extractor.extract(file, '|')
            else:
                df = extractor.extract(file)

            self.logger.log('info', f'Extracted {file_type}: \ncolumns => {list(df.columns)} \nrows => {df.shape[0]}')       

            # Validate the DataFrame
            self.validator.validate(df, file_type)
            
            # Dynamically select the correct transformer based on file type
            transformer = self.transformers.get(file_type)
            if transformer:
                df = transformer.transform(df)
                self.logger.log('info', f"Transformed {file_type}: \ncolumns => {list(df.columns)} \nrows => {df.shape[0]}")
            else:
                self.logger.log('error', f"Unsupported file type for transformation: {file_type}")
                raise ValueError(f"Unsupported file type for transformation: {file_type}")
            
            self.parquet_loader.load(df, f'{file.split("/")[-1].split(".")[0]}')
            self.hdfs_loader.load(hdfspath=f'/stage/{file_type}', 
                                    local_path=f'/home/hadoop/tmp/{file.split("/")[-1].split(".")[0]}.parquet')
            
            self.logger.log('info', f"Pipeline completed successfully for file: {file_type} \n {'='*250}")

        except Exception as e:
            self.logger.log('error', f"Pipeline failed for file: {file_type} with error: \n{e} \n {'='*250}")
            shutil.move(file, f'./data/failed_files/{file.split("/")[-1]}')
            self.notifier.notify(os.getenv('TO_EMAIL_1'))
            
