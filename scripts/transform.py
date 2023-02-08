#!/usr/bin/env python3

import pandas as pd
import sys
import os
from datetime import datetime

# Get the extraction date and check if the data is present
if len(sys.argv) == 1:
    extraction_date = datetime.today()
else:
    try:
        extraction_date = datetime.strptime(sys.argv[1], '%Y-%m-%d')
    except:
        print('''
        Please provide a date in the format 'YYYY-MM-DD' or no date at 
        all to extract today's data.
        ''')
        sys.exit(1)
extraction_date_str = extraction_date.strftime('%Y-%m-%d')
date_folder_path = f'data/postgres/{extraction_date_str}'
csv_folder_path = f'data/csv/{extraction_date_str}'
loading_data_available = (os.path.exists(date_folder_path)) and \
    (os.path.exists(csv_folder_path))
if not loading_data_available:
    print('''
    There is no data in the local disk for the provided date.
    Plese consider extracting the respective date before attempting
    to load it to the final database.
    ''')
    sys.exit(1)

orders_data = pd.read_csv(
    f'{date_folder_path}/orders.csv'
)
order_details_data = pd.read_csv(
    f'{csv_folder_path}/order_details.csv'
)

# treating the column 'shipped date' in orders table
# which has 'nan' values that cannot be converted to a date type
nan_dates = orders_data['shipped_date'].isna()
print(orders_data[nan_dates])

