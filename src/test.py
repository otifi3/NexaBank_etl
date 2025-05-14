import pandas as pd

# df = pd.DataFrame({"bill_id": [0, 1]})            

# df.to_parquet('/home/otifi/ITI/NexaBank/state/credit_cards_billing.parquet', index=False)


# df = pd.DataFrame({"customer_id": [0, 1]})   

# df.to_parquet('/home/otifi/ITI/NexaBank/state/customer_profiles.parquet', index=False)


# df = pd.DataFrame({"ticket_id": [0, 1]})
# df.to_parquet('/home/otifi/ITI/NexaBank/state/support_tickets.parquet', index=False)

print(pd.read_parquet('/home/otifi/ITI/NexaBank/state/support_tickets.parquet'))