import os
import pandas as pd
import numpy as np
import random
import faker
import json
from datetime import datetime, timedelta

# Get current date & hour for partitions
now = datetime.now()
date_partition = now.strftime('%Y-%m-%d')  
hour_partition = now.strftime('%H')       
timestamp_suffix = now.strftime('%Y%m%d%H%M%S')
# Base directory with partitions
base_dir = os.path.join('data/incomming_data', date_partition, hour_partition)
os.makedirs(base_dir, exist_ok=True) 

def make_filepath(base_name: str, ext: str) -> str:
    """Construct full path with timestamp before extension"""
    return os.path.join(base_dir, f"{base_name}_{timestamp_suffix}.{ext}")

# Initialize Faker
fake = faker.Faker()

# Settings: Limit the number of records to 200
NUM_CUSTOMERS = 200
NUM_TICKETS = 200
NUM_MONTHS = 2


cities = ['Cairo', 'Alexandria', 'Riyadh', 'Jeddah', 'Dubai', 'Abu Dhabi', 'Casablanca', 'Doha', 'Beirut', 'Sfax']
complaint_categories = ['Unauthorized Transaction', 'Delayed Refund', 'Card Not Working',
                        'Loan Application Rejected', 'Account Locked', 'Incorrect Charges', 'Mobile App Issues',
                        'Poor Customer Service', 'ATM Withdrawal Failed', 'KYC Verification Delay']

genders = ['Male', 'Female']
customer_tiers = ['Gold', 'Platinum', 'Silver']
product_types = ["CreditCard", "Savings", "PremiumAccount"]

# Create customer ID counter
customer_id_counter = 1

# 1. Generate customer_profiles.csv
customer_profiles = {
    'customer_id': [], 'name': [], 'gender': [], 'age': [], 'city': [],
    'account_open_date': [], 'product_type': [], 'customer_tier': []
}
for i in range(1, NUM_CUSTOMERS + 1):
    customer_profiles['name'].append(fake.name())
    customer_profiles['gender'].append(random.choice(genders))
    customer_profiles['age'].append(random.randint(18, 80))
    customer_profiles['city'].append(random.choice(cities))
    customer_profiles['account_open_date'].append(fake.date_between(start_date='-10y', end_date='-1y'))
    customer_profiles['product_type'].append(random.choice(product_types))
    customer_profiles['customer_tier'].append(random.choice(customer_tiers))
    customer_profiles['customer_id'].append(f'CUST{customer_id_counter:06d}')
    customer_id_counter += 1  # Increment customer ID counter

customer_profiles_df = pd.DataFrame(customer_profiles)
customer_profiles_df.to_csv(make_filepath('customer_profiles', 'csv'), index=False)

# Create ticket ID counter
ticket_id_counter = 1

# 2. Generate support_tickets.csv
support_tickets = {
    'ticket_id': [], 'customer_id': [], 'complaint_category': [], 'complaint_date': [], 'severity': []
}
sampled_customers = random.sample(customer_profiles_df['customer_id'].tolist(), NUM_TICKETS)
for i, cust_id in enumerate(sampled_customers):
    support_tickets['ticket_id'].append(f'TICKET{ticket_id_counter:06d}')
    support_tickets['customer_id'].append(cust_id)
    support_tickets['complaint_category'].append(random.choice(complaint_categories))
    support_tickets['complaint_date'].append(fake.date_between(start_date='-1y', end_date='today'))
    support_tickets['severity'].append(random.randint(0, 10))
    ticket_id_counter += 1  # Increment ticket ID counter

support_tickets_df = pd.DataFrame(support_tickets)
support_tickets_df.to_csv(make_filepath('support_tickets', 'csv'), index=False)

# Create bill ID counter
bill_id_counter = 1

# 3. Generate credit_cards_billing.csv (2 months)
credit_cards_billing = {
    'bill_id': [], 'customer_id': [], 'month': [], 'amount_due': [],
    'amount_paid': [], 'payment_date': []
}

for cust_id in customer_profiles_df['customer_id']:
    for month_offset in range(NUM_MONTHS):
        bill_month = pd.Timestamp('2023-01-01') + pd.DateOffset(months=month_offset)
        amount_due = round(random.uniform(10, 300), 2)
        payment_delay_days = random.choice([0, 0, 0, 1, 2, 5, 7])
        amount_paid = amount_due if payment_delay_days <= 5 else round(amount_due * random.uniform(0.8, 1.0), 2)
        payment_date = (bill_month + pd.DateOffset(days=payment_delay_days)).strftime('%Y-%m-%d')
        credit_cards_billing['bill_id'].append(f'BILL{bill_id_counter:07d}')
        credit_cards_billing['customer_id'].append(cust_id)
        credit_cards_billing['month'].append(bill_month.strftime('%Y-%m'))
        credit_cards_billing['amount_due'].append(amount_due)
        credit_cards_billing['amount_paid'].append(amount_paid)
        credit_cards_billing['payment_date'].append(payment_date)
        bill_id_counter += 1  # Increment bill ID counter

billing_df = pd.DataFrame(credit_cards_billing)
billing_df.to_csv(make_filepath('credit_cards_billing', 'csv'), index=False)

# 4. Generate Money Transfers or Purchases data with datetime timestamp
transactions_data = []
for cust_id in customer_profiles_df['customer_id']:
    if len(transactions_data) >= 200:  # Limit the number of records
        break
    transaction_amount = random.randint(1, 100)
    receiver = np.random.choice(customer_profiles_df['customer_id'])
    transaction_date_only = fake.date_between(start_date='-1y', end_date='today')
    random_time = timedelta(
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
        seconds=random.randint(0, 59)
    )
    transaction_datetime = datetime.combine(transaction_date_only, datetime.min.time()) + random_time

    transactions_data.append({
        'sender': cust_id,
        'receiver': receiver,
        'transaction_amount': transaction_amount,
        'transaction_date': transaction_datetime.strftime('%Y-%m-%d %H:%M:%S')
    })

with open(make_filepath('transactions', 'json'), 'w') as f:
    json.dump(transactions_data, f, indent=4)

# 5. Generate loan requests data with datetime timestamp
loan_types = ["Personal Loan", "Auto Loan", "Home Loan",
              "Credit Card Loan", "Education Loan", "Business Loan",
              "Medical Loan", "Travel Loan", "Top-Up Loan", "Loan Against Deposit"]

loan_data = []
for _ in range(200):  # Limit to 200 records
    customer_id = np.random.choice(customer_profiles_df['customer_id'])
    loan_type = np.random.choice(loan_types)
    amount_utilized = random.randint(10, 1000) * 1000
    utilization_date_only = fake.date_between(start_date='-1y', end_date='today')
    random_time = timedelta(
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
        seconds=random.randint(0, 59)
    )
    utilization_datetime = datetime.combine(utilization_date_only, datetime.min.time()) + random_time
    loan_reason = fake.sentence()  # Generate a random loan reason
    loan_data.append({
        'customer_id': customer_id,
        'loan_type': loan_type,
        'amount_utilized': amount_utilized,
        'utilization_date': utilization_datetime.strftime('%Y-%m-%d %H:%M:%S'),
        'loan_reason': loan_reason
    })

with open(make_filepath('loans', 'json'), 'w') as f:
    json.dump(loan_data, f, indent=4)
