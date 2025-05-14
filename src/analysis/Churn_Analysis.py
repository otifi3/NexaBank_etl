import os
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
from pyhive import hive

# === Set up directories ===
base_dir = '/home/hadoop/data/Churn_Analysis'
data_dir = os.path.join(base_dir, "transformed_data")
output_dir = os.path.join(base_dir, "analysis_outputs")
os.makedirs(data_dir, exist_ok=True)
os.makedirs(output_dir, exist_ok=True)

# === Connect to Hive ===
conn = hive.Connection(
    host='localhost',
    port=10000,
    database='default',
)
cursor = conn.cursor()

# === Explore Hive database ===
cursor.execute("SHOW DATABASES")
print("Databases:", cursor.fetchall())

cursor.execute("USE nexabank_ds")

cursor.execute("SHOW TABLES")
print("Tables:", cursor.fetchall())

for table in ["credit_cards_billing", "loans", "support_tickets", "transactions", "customer_profiles"]:
    print(f"\nSchema of table: {table}")
    cursor.execute(f"DESCRIBE {table}")
    print(cursor.fetchall())

cursor.execute("SELECT current_database()")
print("Current database:", cursor.fetchall())

# === Download data from Hive ===
print("\nDownloading data from Hive tables...")

def save_query_to_parquet(query, filename):
    df = pd.read_sql(query, conn)
    df.to_parquet(os.path.join(data_dir, filename), index=False)
    print(f"Saved {filename}")

save_query_to_parquet("SELECT * FROM customer_profiles", "customer_profiles_time.parquet")
save_query_to_parquet("SELECT * FROM credit_cards_billing", "credit_cards_billing_time.parquet")
save_query_to_parquet("SELECT * FROM transactions", "transactions_time.parquet")

# Load data
profiles = pd.read_parquet(os.path.join(base_dir, "transformed_data", "customer_profiles_time.parquet"))
billing = pd.read_parquet(os.path.join(base_dir, "transformed_data", "credit_cards_billing_time.parquet"))
transactions = pd.read_parquet(os.path.join(base_dir, "transformed_data", "transactions_time.parquet"))

# Convert dates
transactions['transaction_date'] = pd.to_datetime(transactions['transaction_date'])
billing['payment_date'] = pd.to_datetime(billing['payment_date'])
profiles['account_open_date'] = pd.to_datetime(profiles['account_open_date'])

# Last transaction per customer
last_txn = transactions.groupby('sender')['transaction_date'].max().reset_index()
last_txn.columns = ['customer_id', 'last_transaction_date']

# Merge with profiles
df = profiles.merge(last_txn, on='customer_id', how='left')

# Define churn
cutoff_date = datetime.now() - timedelta(days=90)
df['is_churned'] = df['last_transaction_date'] < cutoff_date
df['is_churned'] = df['is_churned'].fillna(True)

# Age group segmentation
df['age_group'] = pd.cut(df['age'], bins=[0, 25, 35, 50, 100], labels=['<25', '25-35', '35-50', '50+'])

# Churn rate by city and age group
churn_by_city = df.groupby('city')['is_churned'].mean().reset_index()
churn_by_age_group = df.groupby('age_group')['is_churned'].mean().reset_index()

# Merge billing
late_pay = billing.groupby('customer_id')['late_days'].mean().reset_index()
df = df.merge(late_pay, on='customer_id', how='left')

# Spending levels
spending = transactions.groupby('sender')['transaction_amount'].sum().reset_index()
spending.columns = ['customer_id', 'total_spending']
df = df.merge(spending, on='customer_id', how='left')
df['spending_level'] = pd.qcut(df['total_spending'], q=3, labels=['Low', 'Medium', 'High'])

# Combined churn analysis
churn_combined = df.groupby(['age_group', 'city', 'spending_level'])['is_churned'].mean().reset_index()
churn_combined_sorted = churn_combined.sort_values(by='is_churned', ascending=False)

# === Save data outputs ===
df.to_csv(os.path.join(output_dir, "churn_customer_analysis.csv"), index=False)
churn_by_city.to_csv(os.path.join(output_dir, "churn_by_city.csv"), index=False)
churn_by_age_group.to_csv(os.path.join(output_dir, "churn_by_age_group.csv"), index=False)
df[['customer_id', 'late_days', 'is_churned']].to_csv(os.path.join(output_dir, "late_days_analysis.csv"), index=False)
churn_combined_sorted.to_csv(os.path.join(output_dir, "top_churn_segments.csv"), index=False)

# === Save plots ===
# 1. Late days distribution
plt.figure(figsize=(10, 6))
sns.histplot(data=df, x='late_days', hue='is_churned', multiple="stack", kde=True)
plt.title("Distribution of Late Payment Days for Churned vs Active Customers")
plt.xlabel("Late Payment Days")
plt.ylabel("Frequency")
plt.savefig(os.path.join(output_dir, "late_days_distribution.png"), dpi=300)
plt.close()

# 2. Heatmap of churn rate by city and age group
churn_heatmap = df.pivot_table(index='age_group', columns='city', values='is_churned', aggfunc='mean')
plt.figure(figsize=(12, 7))
sns.heatmap(churn_heatmap, annot=True, cmap='coolwarm', cbar=True)
plt.title('Churn Rate by Age Group and City')
plt.savefig(os.path.join(output_dir, "churn_heatmap_by_city_age.png"), dpi=300)
plt.close()

# 3. Stacked bar chart of churn count
churn_count = df.groupby(['age_group', 'city'])['is_churned'].value_counts().unstack()
churn_count.plot(kind='bar', stacked=True, figsize=(12, 7))
plt.title('Churn Rate by Age Group and City (Stacked)')
plt.ylabel('Number of Customers')
plt.xlabel('Age Group and City')
plt.tight_layout()
plt.savefig(os.path.join(output_dir, "stacked_churn_by_age_city.png"), dpi=300)
plt.close()

# 4. Churn by age group and spending level
df.groupby(['age_group', 'spending_level'])['is_churned'].mean().unstack().plot(kind='bar', figsize=(10, 6))
plt.title("Churn Rate by Age Group and Spending Level")
plt.ylabel("Churn Rate")
plt.savefig(os.path.join(output_dir, "churn_by_age_spending.png"), dpi=300)
plt.close()

# 5. Top 10 churn segments barplot
plt.figure(figsize=(14, 7))
sns.barplot(
    data=churn_combined_sorted.head(10),
    x='is_churned',
    y='age_group',
    hue='spending_level',
    palette='Reds'
)
plt.title("Top Churn Segments by Age Group, City, and Spending Level")
plt.xlabel("Churn Rate")
plt.ylabel("Age Group")
plt.legend(title='Spending Level')
plt.tight_layout()
plt.savefig(os.path.join(output_dir, "top_churn_segments.png"), dpi=300)
plt.close()

print(f"\n All analysis results and charts saved to:\n{output_dir}")


