import subprocess
import os
import pandas as pd


df = pd.read_parquet("/home/hadoop/tmp/credit_cards_billing_time1.parquet")
print(df.head())
print(df.columns)   
print(df.dtypes)
print(df.shape)
print(df.info())