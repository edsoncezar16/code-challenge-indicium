#!/usr/bin/env python3

import csv
from get_credentials import CREDENTIALS_PATH, get_db_credentials
from sqlalchemy import create_engine, text, MetaData

OUTPUT_DB_NAME = 'order_details'

_, user, password, port = get_db_credentials(CREDENTIALS_PATH)

engine = create_engine(
    f"postgresql://{user}:{password}@localhost/{OUTPUT_DB_NAME}",
    execution_options={'isolation_level': 'AUTOCOMMIT'}
)
metadata = MetaData()
metadata.reflect(bind=engine)
orders = metadata.tables['orders']
order_details = metadata.tables['order_details']
orders_columns = [column.name for column in orders.columns]
order_details_columns = [column.name for column in order_details.columns]
columns = orders_columns + order_details_columns

conn = engine.connect()
query = text(
    '''
    SELECT * 
    FROM ORDERS
    JOIN ORDER_DETAILS
    ON ORDERS.ORDER_ID = ORDER_DETAILS.ORDER_ID
    '''
)
results = conn.execute(query)
with open('data/results.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(columns)
    for row in results:
        writer.writerow(row)
conn.close()
engine.dispose()
