import pandas as pd

df = pd.DataFrame({"bill_id": ["", ""]})            

df.to_parquet('/home/hadoop/state/credit_cards_billing.parquet', index=False)


df = pd.DataFrame({"customer_id": ["", ""]})   

df.to_parquet('/home/hadoop/state/customer_profiles.parquet', index=False)


df = pd.DataFrame({"ticket_id": ["", ""]})
df.to_parquet('/home/hadoop/state/support_tickets.parquet', index=False)

df = pd.DataFrame({"transaction_date": ["0000-00-00"]})
df.to_parquet('/home/hadoop/state/transactions.parquet', index=False)

df = pd.DataFrame({"utilization_date": ["0000-00-00"]})
df.to_parquet('/home/hadoop/state/loans.parquet', index=False)


print(pd.read_parquet('/home/hadoop/state/credit_cards_billing.parquet').head())
print(pd.read_parquet('/home/hadoop/state/customer_profiles.parquet').head())
print(pd.read_parquet('/home/hadoop/state/support_tickets.parquet').head())
print(pd.read_parquet('/home/hadoop/state/transactions.parquet').head())
print(pd.read_parquet('/home/hadoop/state/loans.parquet').head())