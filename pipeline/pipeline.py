from dotenv import load_dotenv
import os

class Pipeline:
    def __init__(self, logger):
        load_dotenv()
        self.user = os.getenv("EMAIL_USER")
        self.password = os.getenv("EMAIL_PASSWORD")
        self.logger = logger

    def run(self, file):
        print(f"Processing file: {file}")
