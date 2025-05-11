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
    utilization_date INT,
    total_cost FLOAT,
    processing_time STRING,
    partition_date DATE,
    partition_hour INT
)
STORED AS PARQUET
LOCATION '/stage/loans';


-- create table for money transfers
CREATE EXTERNAL TABLE money_transfers (
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
LOCATION '/stage/money_transfers';


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


-- create table for credit cards billing
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



