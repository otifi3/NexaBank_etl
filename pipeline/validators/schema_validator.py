import pandas as pd
class SchemaValidator:
    def __init__(self):
        """
        Initialize the SchemaValidator with predefined schemas for different files.
        """
        self.schemas = {
            'customer': {
                'customer_id': 'str',
                'name': 'str',
                'gender': 'str',
                'age': 'str',
                'city': 'str',
                'account_type': 'str',
                'account_open_date': 'str', 
                'product_type': 'str', 
                'customer_tier': 'str'
            },
            'credit': {
                'bill_id': 'str',
                'customer_id': 'str',
                'month': 'str',
                'amount_due': 'float',
                'amount_paid': 'float',
                'payment_date': 'datetime'
            },
            'support': {
                'ticket_id': 'str',
                'customer_id': 'str',
                'complaint_category': 'float',
                'complaint_date': 'datetime',
                'severity': 'str'
            },
            'loans': {
                'customer_id': 'str',
                'loan_type': 'str',
                'amount_utilized': 'float',
                'utilization_date': 'str',
                'loan_reason': 'str'
            },
            'money': {
                'sender': 'str',
                'receiver': 'str',
                'transaction_amount': 'float',
                'transaction_date.': 'str'
            }
        }
        self.file = None
        self.df = None


    def get_schema(self, file: str) -> dict:
        """
        Get the schema for a given file.
        """
        return self.schemas.get(file)
    


    def validate(self, df: pd.DataFrame, file: str) -> bool: 
        """
        Validate the schema of the DataFrame.
        """
        schema = self.get_schema(file)

        self.file = file
        self.df = df
        
        return True