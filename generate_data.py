# In file: enterprise_data_foundation_for_banking/generate_data.py

import pandas as pd
import numpy as np
import datetime
import os

# --- Configuration ---
NUM_CUSTOMERS = 10000
NUM_ACCOUNTS = 15000
DAYS_OF_TRANSACTIONS = 30
START_DATE = datetime.date(2025, 1, 1) # Start date for transactions
DATA_PATH = "source_data/"

def generate_customer_master():
    """Generates static customer information."""
    customer_ids = [f'CUST{i:05}' for i in range(1, NUM_CUSTOMERS + 1)]
    
    data = {
        'customer_id': customer_ids,
        'gender': np.random.choice(['M', 'F', 'O'], NUM_CUSTOMERS, p=[0.45, 0.50, 0.05]),
        'region': np.random.choice(['North', 'South', 'East', 'West'], NUM_CUSTOMERS, p=[0.3, 0.3, 0.2, 0.2]),
        'joining_date': pd.to_datetime(pd.date_range('2018-01-01', periods=NUM_CUSTOMERS, freq='D')[:NUM_CUSTOMERS].strftime('%Y-%m-%d')),
        'is_premium': np.random.choice([True, False], NUM_CUSTOMERS, p=[0.2, 0.8]),
    }
    df = pd.DataFrame(data)
    df.to_csv(f'{DATA_PATH}customer_master.csv', index=False)
    print(f"Generated {len(df)} customer records.")
    return df

def generate_account_balances():
    """Generates initial account balances linked to customers."""
    
    # Create a list of account-customer pairs (some customers have multiple accounts)
    account_customer_map = []
    customer_ids = [f'CUST{i:05}' for i in range(1, NUM_CUSTOMERS + 1)]
    
    # Ensure at least one account per customer
    for cust_id in customer_ids:
        account_customer_map.append({'account_id': f'ACC{len(account_customer_map) + 1:06}', 'customer_id': cust_id})

    # Add extra accounts for multi-account simulation
    extra_accounts_needed = NUM_ACCOUNTS - len(account_customer_map)
    extra_cust_ids = np.random.choice(customer_ids, extra_accounts_needed)
    for i in range(extra_accounts_needed):
        account_customer_map.append({'account_id': f'ACC{len(account_customer_map) + 1:06}', 'customer_id': extra_cust_ids[i]})

    df = pd.DataFrame(account_customer_map)
    df['account_type'] = np.random.choice(['Savings', 'Checking', 'Loan'], len(df), p=[0.6, 0.3, 0.1])
    df['current_balance'] = np.random.normal(loc=10000, scale=8000, size=len(df)).clip(lower=100)
    
    df.to_csv(f'{DATA_PATH}account_balances.csv', index=False)
    print(f"Generated {len(df)} account balance records.")
    return df

def generate_transaction_data(accounts_df):
    """Generates daily transactions for all accounts over a period."""
    
    all_transactions = []
    date_range = [START_DATE + datetime.timedelta(days=i) for i in range(DAYS_OF_TRANSACTIONS)]
    total_tx_count = 0
    
    for day in date_range:
        # Simulate a variable number of transactions per day
        daily_tx_count = len(accounts_df) * np.random.randint(1, 4)
        total_tx_count += daily_tx_count

        daily_accounts = np.random.choice(accounts_df['account_id'], daily_tx_count, replace=True)
        tx_amounts = np.random.normal(loc=150, scale=100, size=daily_tx_count).clip(lower=1)
        tx_types = np.random.choice(['DEPOSIT', 'WITHDRAWAL', 'TRANSFER', 'PURCHASE'], daily_tx_count, p=[0.25, 0.35, 0.20, 0.20])
        
        daily_tx = pd.DataFrame({
            'transaction_id': [f'TX{total_tx_count + i:09}' for i in range(daily_tx_count)],
            'account_id': daily_accounts,
            'transaction_date': day,
            'amount': tx_amounts,
            'type': tx_types,
            'source_channel': np.random.choice(['WEB', 'MOBILE', 'ATM', 'BRANCH'], daily_tx_count, p=[0.4, 0.3, 0.2, 0.1])
        })
        all_transactions.append(daily_tx)

    transactions_df = pd.concat(all_transactions)
    transactions_df.to_csv(f'{DATA_PATH}transaction_data.csv', index=False)
    print(f"Generated {len(transactions_df)} total transaction records over {DAYS_OF_TRANSACTIONS} days.")

if __name__ == '__main__':
    # Ensure the data directory exists
    os.makedirs(DATA_PATH, exist_ok=True)
    
    # 1. Generate Customer Master
    customer_df = generate_customer_master()
    
    # 2. Generate Account Balances
    accounts_df = generate_account_balances()
    
    # 3. Generate Transaction Data (linked to accounts)
    generate_transaction_data(accounts_df)
    
    print("\nData generation complete. Files are in 'source_data/'.")