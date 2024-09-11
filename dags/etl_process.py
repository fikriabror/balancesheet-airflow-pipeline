import os
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook


# Extract Data
def extract_data(**kwargs):
    data_dir = 'opt/airflow/data'
    file_path = os.path.join(data_dir, '[Confidential] Mekari - Data Engineer Senior.xlsx')
    df = pd.read_excel(file_path, engine='openpyxl')
    return df

# Transform data
def transform_data(**kwargs):
    # using xcom to get the extracted data
    df = kwargs['ti'].xcom_pull(task_ids='extract_data')
    
    # Cleaning the header of the column
    df.columns = df.columns.str.lower().str.replace(' ', '_').str.replace('.', '_')
    
    # drop the last column which has no value
    df.drop(columns=['_'], inplace= True)
    
    # Transform the account_no column from string to int
    df['account_no'] = df['account_no'].str.replace("'",'').astype(int)
    
    # Transform numeric column from null value to 0, to make calculatable
    df['withdrawal_amt']=df['withdrawal_amt'].fillna(value=0)
    df['deposit_amt']=df['deposit_amt'].fillna(value=0)
    df['balance_amt']=df['balance_amt'].fillna(value=0)
    
    # add id auto increment
    df['id'] = range(1, len(df) + 1)
    
    return df

# Load Data
def load_data(**kwargs):
    df = kwargs['ti'].xcom_pull(task_ids='transform_data')
    
    # connect to postgreSql
    pg_hook = PostgresHook(postgres_conn_id='local_postgres')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    # Create table
    
    create_table_query="""
    CREATE TABLE IF NOT EXISTS balance_sheet(
        id SERIAL PRIMARY KEY,
        account_no INT,
        date DATE,
        transaction_details TEXT,
        value_date DATE,
        withdrawal_amt FLOAT,
        deposit_amt FLOAT,
        balance_amt FLOAT,
        CONSTRAINT unique_balance UNIQUE (account_no, date, transaction_details)
    );
    """
    
    cursor.execute(create_table_query)
    
    # Insert df to postgres
    for index, row in df.iterrows():
        insert_query="""
        INSERT INTO balance_sheet (account_no, date, transaction_details, value_date, withdrawal_amt, deposit_amt, balance_amt)
        VALUES (%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (account_no, date, transaction_details) DO UPDATE
        SET withdrawal_amt = EXCLUDED.withdrawal_amt,
            value_date = EXCLUDED.value_date,
            deposit_amt = EXCLUDED.deposit_amt,
            balance_amt = EXCLUDED.balance_amt;
        """ 
        cursor.execute(insert_query, (
            row['account_no'], row['date'], row['transaction_details'],
            row['value_date'],row['withdrawal_amt'],row['deposit_amt'],row['balance_amt']
        ))
    
    # close the connection
    conn.commit()
    cursor.close()
    conn.close()
    

# calculated_table
def fact_balance_sheet(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id='local_postgres')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    df=kwargs['ti'].xcom_pull(task_ids='transform_data')
    
    df['year']= pd.to_datetime(df['date']).dt.year
    df['month']= pd.to_datetime(df['date']).dt.month
    
    # calculate total withdrawal and deposit amounts
    summary_df = df.groupby(['account_no', 'year', 'month']).agg(
        total_withdrawal_amt=pd.NamedAgg(column='withdrawal_amt', aggfunc='sum'),
        total_deposit_amt=pd.NamedAgg(column='deposit_amt', aggfunc='sum')
    ).reset_index()
    
    # determine the ending balance amount
    df['date'] = pd.to_datetime(df['date'])
    
    df = df.sort_values(by=['account_no', 'year', 'month', 'date'])
    ending_balance_df = df.groupby(['account_no', 'year', 'month']).tail(1)
    
    # Merge the get ending balance to the summary
    
    summary_df=summary_df.merge(ending_balance_df[['account_no', 'year', 'month', 'balance_amt']],
                                on=['account_no', 'year', 'month'],
                                how='left'
                                )
    summary_df.rename(columns={'balance_amt': 'ending_balance_amt'}, inplace=True)
    
    #load summary 
    create_table_query="""
    CREATE TABLE IF NOT EXISTS balance_sheet(
        id SERIAL PRIMARY KEY,
        account_no INT,
        year INT,
        month INT,
        total_withdrawal_amt FLOAT,
        total_deposit_amt FLOAT,
        ending_balance_amt FLOAT
    );
    """
    
    cursor.execute(create_table_query)
    
    # Delete existing data
    delete_query = "DELETE FROM fact_balance_sheet;"
    cursor.execute(delete_query)
    
    for index, row in summary_df.iterrows():
        insert_table_query="""
        INSERT INTO fact_balance_sheet (account_no, year, month, total_withdrawal_amt, total_deposit_amt, ending_balance_amt)
        VALUES(%s,%s,%s,%s,%s,%s)
        """
        
        cursor.execute(insert_table_query, (
            row['account_no'], row['year'], row['month'], row['total_withdrawal_amt'],
            row['total_deposit_amt'],row['ending_balance_amt']))
        
    # commit transaction
    conn.commit()
    cursor.close()
    conn.close()