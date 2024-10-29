import os
import re
import pandas as pd
import numpy as np
import psycopg2
from datetime import datetime
from dotenv import load_dotenv

# database config params
load_dotenv()

db_config = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT")
}

# get data from database
def get_data(query):
    connection = psycopg2.connect(**db_config)
    try:
        df = pd.read_sql_query(query, connection)
    except Exception as e:
        print("Database connection failed due to {}".format(e)) 
    finally:
        connection.close()

    return df

# get data from database
def get_data(query):
    connection = psycopg2.connect(**db_config)
    try:
        df = pd.read_sql_query(query, connection)
    except Exception as e:
        print("Database connection failed due to {}".format(e)) 
    finally:
        connection.close()

    return df

# load all datasets
users_df = get_data("SELECT * FROM users")
trades_df = get_data("SELECT * FROM trades")

print("Users DataFrame Description:")
print(users_df.describe())

print("\nTrades DataFrame Description:")
print(trades_df.describe())

################# Test Functions ################# 

def check_for_nulls(df):
    null_counts = df.isnull().sum()
    if null_counts.any():
        print(f"NULL values detected at column:\n{null_counts[null_counts > 0]}")

check_for_nulls(users_df)
check_for_nulls(trades_df)
# Missing values detected in the trade table contractsize column, they should be removed or replaced.



# Check for unexpected numerical values

# check for numerical range
def check_unexpected_numerical(df, column, min_value, max_value):
    unexpected_values = df[(df[column] < min_value) | (df[column] > max_value)]
    if not unexpected_values.empty:
        print(f"Unexpected numerical values in {column}:\n{unexpected_values}")

# check for specific values
def check_specific_numerical_values(df, column, allowed_values):
    unexpected_values = df[~df[column].isin(allowed_values)]
    if not unexpected_values.empty:
        print(f"Unexpected values in {column}:\n{unexpected_values}")

# Check if all values in the column are integers
def check_all_integers(df, column):
    if not df[column].apply(lambda x: isinstance(x, int) or (isinstance(x, float) and x.is_integer())).all():
        print(f"Non-integer values found in {column}")


check_specific_numerical_values(users_df, 'enable', allowed_values={0,1})     # 'enable' should be either 0 or 1
check_specific_numerical_values(trades_df, 'cmd', allowed_values={0,1})     # 'cmd' should be either 0 or 1
check_specific_numerical_values(trades_df, 'contractsize', allowed_values={1, 10, 100, 1000, 100000})  # it seems 'contractsize' should be like this

check_all_integers(trades_df, 'digits') # 'digits' should be integers
check_all_integers(trades_df, 'volume') # 'volume' should be integers

# It seems there are no unexpected values in numerical columns. 
# However, there seems to be an invalid symbol called "COFFEE" that should be removed.


# check for unexpected dates

def detect_close_before_open(df, open_column='open_time', close_column='close_time'):
    # Identify rows where close_date is before open_date
    anomalies = df[df[close_column] < df[open_column]]
    if not anomalies.empty:
        print(f"Rows with {close_column} prior to {open_column}:\n{anomalies}")

detect_close_before_open(trades_df)
# No close before open time.


# Check for unexpected strings

def check_hash_values(df, column, expected_length=32):
    # Regular expression for a valid hexadecimal string of the specified length
    hash_pattern = re.compile(r'^[A-F0-9]{' + str(expected_length) + r'}$')
    
    # Check each value in the column for compliance with the hash format
    invalid_hashes = df[~df[column].apply(lambda x: bool(hash_pattern.match(x)))]
    
    if not invalid_hashes.empty:
        print(f"Invalid hash values in column '{column}':\n{invalid_hashes}")
    else:
        print(f"All values in column '{column}' are valid hashes of length {expected_length}.")

check_hash_values(trades_df, 'login_hash')
check_hash_values(trades_df, 'ticket_hash')
check_hash_values(trades_df, 'server_hash')
check_hash_values(users_df, 'login_hash')
check_hash_values(users_df, 'country_hash')
check_hash_values(users_df, 'server_hash')
# No incorrect hash tags


def check_unexpected_symbols(df, unexpected_symbol):
    unexpected_rows = df[df['symbol'] == unexpected_symbol]
    if not unexpected_rows.empty:
        print(f"Unexpected symbol '{unexpected_symbol}' found in the trades table:\n", len(unexpected_rows))

check_unexpected_symbols(trades_df, unexpected_symbol="COFFEE")
# there are 7 rows with the wrong symbol "COFFEE", exactly the rows with missing contractsize



# Data Integrity Checks

# check records with the same login_hash value should have the same server_hash value
def check_login_server_consistency(users_df, trades_df):

    combined_df = pd.concat([users_df[['login_hash', 'server_hash']], trades_df[['login_hash', 'server_hash']]])
    inconsistent_logins = combined_df.groupby('login_hash')['server_hash'].nunique()
    
    # Filter to find login_hash entries associated with more than one server_hash
    inconsistent_logins = inconsistent_logins[inconsistent_logins > 1]
    
    if not inconsistent_logins.empty:
        print("The following login_hash values are associated with multiple server_hash values:")
        for login_hash, count in inconsistent_logins.items():
            print(f"login_hash: {login_hash}, unique server_hash count: {count}")
    else:
        print("No inconsistencies found.")

# check records with the same ticket_hash value should have the same server_hash value
def check_ticket_server_consistency(trades_df):

    inconsistent_logins = trades_df.groupby('ticket_hash')['server_hash'].nunique()
    inconsistent_logins = inconsistent_logins[inconsistent_logins > 1]
    
    if not inconsistent_logins.empty:
        print("The following ticket_hash values are associated with multiple server_hash values:")
        for ticket_hash, count in inconsistent_logins.items():
            print(f"ticket_hash: {ticket_hash}, unique server_hash count: {count}")
    else:
        print("No inconsistencies found.")

check_login_server_consistency(users_df, trades_df)
check_ticket_server_consistency(trades_df)
# No inconsistencies found for joining
