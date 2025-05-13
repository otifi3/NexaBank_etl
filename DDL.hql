CREATE DATABASE NexaBank_DS;

-- create table for support tickets
CREATE EXTERNAL TABLE support_tickets (
    ticket_id STRING,
    customer_id STRING,
    complaint_category STRING,
    complaint_date DATE,
    severity INT,
    age INT,
    processing_time STRING,
    partition_date DATE,
    partition_hour INT
)
STORED AS PARQUET
LOCATION '/stage/support_tickets';

-- create table for loans
CREATE EXTERNAL TABLE loans (
    customer_id STRING,
    loan_type STRING,
    amount_utilized FLOAT,
    utilization_date DATE,
    age INT,
    total_cost FLOAT,
    processing_time STRING,
    partition_date DATE,
    partition_hour INT
)
STORED AS PARQUET
LOCATION '/stage/loans';


-- create table for money transfers
CREATE EXTERNAL TABLE transactions (
    sender STRING,
    receiver STRING,
    transaction_amount FLOAT,
    transaction_date DATE,
    cost FLOAT,
    total_amount FLOAT,
    processing_time STRING,
    partition_date DATE,
    partition_hour INT
)
STORED AS PARQUET
LOCATION '/stage/transactions';


-- create table for customer complaints
CREATE EXTERNAL TABLE credit_cards_billing (
    bill_id STRING,
    customer_id STRING,
    month STRING,
    amount_due FLOAT,
    amount_paid FLOAT,
    payment_date DATE,
    late_days INT,
    fine FLOAT,
    total_amount FLOAT,
    processing_time STRING,
    partition_date DATE,
    partition_hour INT
)
STORED AS PARQUET
LOCATION '/stage/credit_cards_billing';

-- create table for customer profiles
CREATE EXTERNAL TABLE customer_profiles (
    customer_id STRING,
    name STRING,
    gender STRING,
    age INT,
    city STRING,
    account_open_date DATE,
    product_type STRING,
    customer_tier STRING,
    tenure INT,
    customer_segment STRING,
    processing_time STRING,
    partition_date DATE,
    partition_hour INT
)
STORED AS PARQUET
LOCATION '/stage/customer_profiles';



-- hdfs dfs -rm -r /stage
-- hdfs dfs -mkdir -p /stage/credit_cards_billing
-- hdfs dfs -mkdir -p /stage/customer_profiles
-- hdfs dfs -mkdir -p /stage/loans
-- hdfs dfs -mkdir -p /stage/support_tickets
-- hdfs dfs -mkdir -p /stage/transactions


