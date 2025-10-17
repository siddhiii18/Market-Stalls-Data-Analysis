# data_generator.py
# Generates synthetic data for Pop-Up Artisan Market Analytics

import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta
import random

# ----------------------------
# Config
# ----------------------------
NUM_STALLS = 20
NUM_DAYS = 7
TX_PER_DAY = 100
PRODUCTS = ['Handmade Soap', 'Artisan Candle', 'Clay Pot', 'Knitted Scarf', 
            'Organic Tea', 'Wooden Toy', 'Leather Wallet', 'Ceramic Mug']
BASE_DIR = "../synthetic_data"

# Create folders
os.makedirs(os.path.join(BASE_DIR, 'transactions'), exist_ok=True)
os.makedirs(os.path.join(BASE_DIR, 'footfall'), exist_ok=True)

# ----------------------------
# Generate stalls.csv
# ----------------------------
stalls = []
for i in range(1, NUM_STALLS + 1):
    stalls.append({
        'stall_id': f'S{i:03}',
        'stall_name': f'Stall {i}',
        'stall_category': random.choice(['Handicraft', 'Food', 'Textile', 'Decor'])
    })

stalls_df = pd.DataFrame(stalls)
stalls_df.to_csv(os.path.join(BASE_DIR, 'stalls.csv'), index=False)
print("stalls.csv created ✅")

# ----------------------------
# Generate transactions & footfall per day
# ----------------------------
start_date = datetime.now() - timedelta(days=NUM_DAYS)

for day in range(NUM_DAYS):
    date = (start_date + timedelta(days=day)).strftime("%Y-%m-%d")
    
    # Transactions
    transactions = []
    for tx in range(TX_PER_DAY):
        stall = random.choice(stalls)
        product = random.choice(PRODUCTS)
        quantity = random.randint(1, 5)
        amount = quantity * random.randint(50, 500)
        tx_time = (start_date + timedelta(days=day, minutes=random.randint(0, 1440))).strftime("%Y-%m-%d %H:%M:%S")
        transactions.append({
            'tx_id': f'TX{day:02}{tx:03}',
            'stall_id': stall['stall_id'],
            'product': product,
            'quantity': quantity,
            'amount': amount,
            'timestamp': tx_time
        })
    tx_df = pd.DataFrame(transactions)
    tx_df.to_csv(os.path.join(BASE_DIR, 'transactions', f'transactions_{date}.csv'), index=False)

    # Footfall
    footfall = []
    for stall in stalls:
        count = random.randint(50, 500)
        footfall.append({
            'stall_id': stall['stall_id'],
            'date': date,
            'visitors': count
        })
    footfall_df = pd.DataFrame(footfall)
    footfall_df.to_csv(os.path.join(BASE_DIR, 'footfall', f'footfall_{date}.csv'), index=False)

print("Transaction & footfall CSVs created ✅")