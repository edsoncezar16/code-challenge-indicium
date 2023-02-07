#!/usr/bin/env python3

from sqlalchemy import create_engine, text, exc, Column, ForeignKey
from sqlalchemy import Integer, Float, String, Date
from sqlalchemy.orm import relationship, declarative_base
from get_credentials import CREDENTIALS_PATH, get_db_credentials
import sys
import os
from datetime import datetime
import pandas as pd

OUTPUT_DB_NAME = 'order_details'

_, user, password, port = get_db_credentials(CREDENTIALS_PATH)

engine = create_engine(
    f"postgresql://{user}:{password}@localhost/postgres",
    execution_options={'isolation_level': 'AUTOCOMMIT'}
)
try:
    conn = engine.connect()
    conn.execute(text(f'create database {OUTPUT_DB_NAME}'))
    conn.close()
except exc.ProgrammingError: # database already exists
    pass  
finally:
    engine.dispose()

engine = create_engine(
    f"postgresql://{user}:{password}@localhost/{OUTPUT_DB_NAME}"
)

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

# drop previous tables and create new ones with current data
Base = declarative_base()
Base.metadata.drop_all(engine)


class Orders(Base):
    __tablename__ = 'orders'
    order_id = Column('order_id', Integer, primary_key=True)
    customer_id = Column('customer_id', String, nullable=False)
    employee_id = Column('employee_id', String, nullable=False)
    order_date = Column('order_date', Date)
    required_date = Column('required_date', Date)
    shipped_date = Column('shipped_date', Date)
    ship_via = Column('ship_via', Integer)
    freight = Column('freight', Float)
    ship_name = Column('ship_name', String)
    ship_address = Column('ship_address', String)
    ship_city = Column('ship_city', String)
    ship_region = Column('ship_region', String)
    ship_postal_code = Column('ship_postal_code', String)
    ship_country = Column('ship_country', String)
    order_details = relationship('OrderDetails', back_populates='orders')


class OrderDetails(Base):
    __tablename__ = 'order_details'
    order_id = Column(
        'order_id', Integer,
        ForeignKey('orders.order_id', ondelete='CASCADE'), primary_key=True
    )
    product_id = Column('product_id', Integer, primary_key=True)
    unit_price = Column('unit_price', Float)
    quantity = Column('quantity', Integer)
    discount = Column('discount', Float)
    orders = relationship('Orders', back_populates='order_details')


Base.metadata.create_all(engine)

# populate tables with the extracted data
orders_data.to_sql('orders', engine, if_exists='append', index=False)
order_details_data.to_sql(
    'order_details', engine, if_exists='append', index=False
)

engine.dispose()  # to prevent resource leakage
