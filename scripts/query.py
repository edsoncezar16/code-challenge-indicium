#!/usr/bin/env python3

import pandas as pd
import os
import sys
from utils import get_db_credentials
from sqlalchemy import create_engine, text, MetaData

DB_HOST = os.environ["DB_HOST"]
CREDENTIALS_PATH = os.environ["CREDENTIALS_PATH"]
OUTPUT_DB_NAME = os.environ["OUTPUT_DB_NAME"]
RESULTS_PATH = os.environ["RESULTS_PATH"]

_, user, password, port = get_db_credentials(CREDENTIALS_PATH)

engine = create_engine(
    f"postgresql://{user}:{password}@{DB_HOST}/{OUTPUT_DB_NAME}",
    execution_options={"isolation_level": "AUTOCOMMIT"},
)
query = text(
    """
    SELECT * 
    FROM ORDERS
    JOIN ORDER_DETAILS
    ON ORDERS.ORDER_ID = ORDER_DETAILS.ORDER_ID
    """
)
try:
    conn = engine.connect()
    results = pd.read_sql_query(query, conn)
    conn.close()
    engine.dispose()
    results = results.T.drop_duplicates().T  # to avoid repetition
    # resulting from the join operation
    results.to_csv(RESULTS_PATH, index=False)
except:  # output database is not set
    print(
        """
        Output database is not set. Please consider loading data to output
        database before querying.
        """
    )
    sys.exit(1)
