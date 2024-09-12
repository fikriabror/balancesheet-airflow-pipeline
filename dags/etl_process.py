import os
import pandas as pd
from datetime import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook


# Extract Data
def extract_data(**kwargs):
    data_dir = '/opt/airflow/data'
    file_path = os.path.join(data_dir, 'dataset.xlsx')
    
    # Check if the file exists
    if not os.path.isfile(file_path):
        raise FileNotFoundError(f"The file {file_path} does not exist.")
    
    df = pd.read_excel(file_path, engine='openpyxl')
    
    output_path = os.path.join(data_dir, 'extracted_data.csv')
    df.to_csv(output_path, index=False)
    
    kwargs['ti'].xcom_push(key='extracted_data_path', value=output_path)
    
    return output_path

# Transform data
def transform_data(**kwargs):
    data_dir = '/opt/airflow/data'
    # using xcom to get the extracted data
    file_path = kwargs['ti'].xcom_pull(task_ids='extract_data', key='extracted_data_path')
    
    #read csv files from xcom
    df = pd.read_csv(file_path)
    
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
    
    transformed_path = os.path.join(data_dir, 'transformed_data.csv')
    df.to_csv(transformed_path, index=False)
    
    kwargs['ti'].xcom_push(key='transformed_data_path', value=transformed_path)
    
    return transformed_path

# Load Data
def load_data(**kwargs):
    file_path = kwargs['ti'].xcom_pull(task_ids='transform_data', key='transformed_data_path')
    
    if not os.path.isfile(file_path):
        raise FileNotFoundError(f"The file {file_path} does not exist.")
    df = pd.read_csv(file_path)
    
    # connect to postgreSql
    pg_hook = PostgresHook(postgres_conn_id='local_postgres')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    # check table whether exists or not
    check_table_exists_query = """
    SELECT EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'public'
        AND table_name = 'balance_sheet'
    );
    """
    
    # Create table
    cursor.execute(check_table_exists_query)
    table_exists = cursor.fetchone()[0]
    
    if not table_exists:       
        create_table_query="""
        CREATE TABLE IF NOT EXISTS balance_sheet(
            id SERIAL PRIMARY KEY,
            account_no BIGINT,
            date DATE,
            transaction_details TEXT,
            value_date DATE,
            withdrawal_amt DOUBLE PRECISION,
            deposit_amt DOUBLE PRECISION,
            balance_amt DOUBLE PRECISION,
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
    
    file_path=kwargs['ti'].xcom_pull(task_ids='transform_data', key='transformed_data_path')
    
    if not os.path.isfile(file_path):
        raise FileNotFoundError(f"The file {file_path} does not exist.")
    
    df = pd.read_csv(file_path)
    
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
    
    summary_df['total_withdrawal_amt'] = summary_df['total_withdrawal_amt'].round(2)
    summary_df['total_deposit_amt'] = summary_df['total_deposit_amt'].round(2)
    summary_df['ending_balance_amt'] = summary_df['ending_balance_amt'].round(2)
    
    #load summary 
    create_table_query="""
    CREATE TABLE IF NOT EXISTS fact_balance_sheet(
        id SERIAL PRIMARY KEY,
        account_no BIGINT,
        year INT,
        month INT,
        total_withdrawal_amt DOUBLE PRECISION,
        total_deposit_amt DOUBLE PRECISION,
        ending_balance_amt DOUBLE PRECISION
    );
    """
    
    cursor.execute(create_table_query)
    
    # check table whether exists or not
    check_table_exists_query = """
    SELECT EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'public'
        AND table_name = 'fact_balance_sheet'
    );
    """
    
    cursor.execute(check_table_exists_query)
    table_exists = cursor.fetchone()[0]
    
    # Delete existing data
    if table_exists:
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