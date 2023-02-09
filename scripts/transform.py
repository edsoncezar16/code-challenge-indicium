#!/usr/bin/env python3

# treats the column 'shipped date' in orders table
# which has 'nan' values that cannot be converted to a date type

import sys
import os
import pandas as pd
from utils import get_operation_date

# Get the extraction date and check if the data is present
operation_date = get_operation_date()
operation_date_str = operation_date.strftime("%Y-%m-%d")
date_folder_path = f"data/postgres/{operation_date_str}"
if not os.path.exists(date_folder_path):
    print(
        """
    There is no data in the local disk for the provided date.
    Plese consider extracting the respective date before attempting
    to transform it.
    """
    )
    sys.exit(1)

# make necessary data transformations
orders_data = pd.read_csv(f"{date_folder_path}/orders.csv")
orders_data["shipped_date"] = pd.to_datetime(
    orders_data["shipped_date"], errors="coerce"
)
orders_data["shipped_date"].fillna(pd.Timestamp("1970-01-01 00:00:00"), inplace=True)
orders_data.to_csv(f"{date_folder_path}/orders.csv", index=False)
