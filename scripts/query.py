#!/usr/bin/env python3

import pandas as pd
from get_credentials import CREDENTIALS_PATH, get_db_credentials
from sqlalchemy import create_engine, text, MetaData

OUTPUT_DB_NAME = 'order_details'
RESULTS_PATH = 'data/results.csv'

_, user, password, port = get_db_credentials(CREDENTIALS_PATH)

engine = create_engine(
    f"postgresql://{user}:{password}@localhost/{OUTPUT_DB_NAME}",
    execution_options={'isolation_level': 'AUTOCOMMIT'}
)
query = text(
    '''
    SELECT * 
    FROM ORDERS
    JOIN ORDER_DETAILS
    ON ORDERS.ORDER_ID = ORDER_DETAILS.ORDER_ID
    '''
)
conn = engine.connect()
results = pd.read_sql_query(query, conn)
conn.close()
engine.dispose()
results = results.T.drop_duplicates().T # to avoid repetition
# resulting from the join operation
results.to_csv(RESULTS_PATH, index=False)
