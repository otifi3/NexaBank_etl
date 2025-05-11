from dotenv import load_dotenv
import os
from pipeline.extractors.csv_extractor import CSVExtractor
from pipeline.extractors.txt_extractor import TXTExtractor
from pipeline.extractors.json_extractor import JSONExtractor

from pipeline.transformers.credit_transformers import CreditTransformers
from pipeline.transformers.customer_transformers import CustomerTransformers
from pipeline.transformers.Loans_transformers import LoanTransformers
from pipeline.transformers.support_transformers import SupportTransformers
from pipeline.transformers.money_transfers_transformers import MoneyTransformers
from pipeline.validators.schema_validator import SchemaValidator
from pipeline.notifier.email_notifier import EmailNotifier 
from pipeline.loaders.hdfs_loader import HDFSLoader 
from pipeline.logger.logger import Logger  

class Pipeline:
    def __init__(self, logger: Logger):
        load_dotenv()  # Load environment variables from .env
        self.user = os.getenv("EMAIL_USER")
        self.password = os.getenv("EMAIL_PASSWORD")
        self.smtp_server = os.getenv("SMTP_SERVER", "smtp.gmail.com") 
        self.smtp_port = os.getenv("SMTP_PORT", 465)

        # Assign the logger
        self.logger = logger

        # Initialize extractors
        self.csv_extractor = CSVExtractor()
        self.txt_extractor = TXTExtractor()
        self.json_extractor = JSONExtractor()

        # Initialize transformers
        self.support_transformers = SupportTransformers()
        self.loan_transformers = LoanTransformers('./etl/pipeline/support/english_words.txt')
        self.money_transformers = MoneyTransformers()
        self.credit_transformers = CreditTransformers()
        self.customer_transformers = CustomerTransformers()

        # Initialize the schema validator and email notifier
        self.validator = SchemaValidator('./etl/pipeline/support/schemas.json')  
        self.notifier = EmailNotifier(self.user, self.password) 
        self.loader = HDFSLoader() 

    def run(self, file):
        """
        Process the file using the appropriate extractor, transformer, validator, and loader.
        """
        self.logger.log('info', f"Processing file: {file}")
        file_extension = file.split('.')[-1].lower()

        try:
            if file_extension == "csv":
                df = self.process_csv(file)
            elif file_extension == "txt":
                df = self.process_txt(file)
            elif file_extension == "json":
                df = self.process_json(file)
            else:
                self.logger.log('error', f"Unsupported file type: {file_extension}")
                return
            
            file_type = file.split('/')[-1].split('.')[0]
            if not self.validator.validate(df, file_type):
                self.logger.log('error', f"Schema validation failed for {file}.")
                raise ValueError(f"Schema validation failed for {file}.")
                
            self.loader.load(df, f'/staging/{file.split("/")[-1].split(".")[0]}')
            self.logger.log('info', f"Successfully processed and loaded {file} to HDFS.")

        except Exception as e:
            self.logger.log('error', f"Pipeline failed for {file}: {str(e)}")
            self.notifier.send_failure_notification(file, str(e))
            raise

    def process_csv(self, file):
        """
        Extract and transform CSV file based on its type.
        """
        if 'credit_cards_billing' in file:
            df = self.csv_extractor.extract(file)
            df = self.credit_transformers.transform(df)
            self.logger.log('info', f"Processed Credit CSV file: {file}")
        elif 'customer_profiles' in file:
            df = self.csv_extractor.extract(file)
            df = self.customer_transformers.transform(df)
            self.logger.log('info', f"Processed Customer CSV file: {file}")
        elif 'support_tickets' in file:
            df = self.csv_extractor.extract(file)
            df = self.support_transformers.transform(df)
            self.logger.log('info', f"Processed Support CSV file: {file}")
        else:
            self.logger.log('error', f"Unsupported CSV file: {file}")
        return df

    def process_txt(self, file):
        """
        Extract and transform TXT file.
        """
        df = self.txt_extractor.extract(file)
        df = self.loan_transformers.transform(df)
        self.logger.log('info', f"Processed TXT file: {file}")
        return df

    def process_json(self, file):
        """
        Extract and transform JSON file.
        """
        df = self.json_extractor.extract(file)
        df = self.money_transformers.transform(df)
        self.logger.log('info', f"Processed JSON file: {file}")
        return df
