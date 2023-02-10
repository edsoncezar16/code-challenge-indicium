#!/usr/bin/env python3

from sqlalchemy import create_engine, text, inspect, exc, Column, ForeignKey
from sqlalchemy import Integer, Float, String, Date
from sqlalchemy.orm import relationship, declarative_base, sessionmaker
from utils import get_db_credentials, get_operation_date
import sys
import os
import pandas as pd

DB_HOST = os.environ["DB_HOST"]
CREDENTIALS_PATH = os.environ["CREDENTIALS_PATH"]
OUTPUT_DB_NAME = os.environ["OUTPUT_DB_NAME"]

_, user, password, port = get_db_credentials(CREDENTIALS_PATH)

engine = create_engine(
    f"postgresql://{user}:{password}@{DB_HOST}/postgres",
    execution_options={"isolation_level": "AUTOCOMMIT"},
)
try:
    conn = engine.connect()
    conn.execute(text(f"create database {OUTPUT_DB_NAME}"))
    conn.close()
except exc.ProgrammingError:  # database already exists
    pass
finally:
    engine.dispose()

engine = create_engine(
    f"postgresql://{user}:{password}@{DB_HOST}/{OUTPUT_DB_NAME}",
    execution_options={"isolation_level": "AUTOCOMMIT"},
)

# Get the operation date and check if the data is present
operation_date = get_operation_date()
operation_date_str = operation_date.strftime("%Y-%m-%d")
date_folder_path = f"data/postgres/{operation_date_str}"
csv_folder_path = f"data/csv/{operation_date_str}"
loading_data_available = (os.path.exists(date_folder_path)) and (
    os.path.exists(csv_folder_path)
)
if not loading_data_available:
    print(
        """
    There is no data in the local disk for the provided date.
    Plese consider extracting the respective date before attempting
    to load it to the final database.
    """
    )
    sys.exit(1)

orders_data = pd.read_csv(f"{date_folder_path}/orders.csv")
order_details_data = pd.read_csv(f"{csv_folder_path}/order_details.csv")

# drop previous tables and create new ones with current data
inspector = inspect(subject=engine)
table_names = inspector.get_table_names()
conn = engine.connect()
for table_name in table_names:
    sql = text(f"drop table {table_name} cascade")
    conn.execute(sql)
conn.close()

Base = declarative_base()


class Orders(Base):
    __tablename__ = "orders"
    order_id = Column("order_id", Integer, primary_key=True)
    customer_id = Column("customer_id", String, nullable=False)
    employee_id = Column("employee_id", String, nullable=False)
    order_date = Column("order_date", Date)
    required_date = Column("required_date", Date)
    shipped_date = Column("shipped_date", Date)
    ship_via = Column("ship_via", Integer)
    freight = Column("freight", Float)
    ship_name = Column("ship_name", String)
    ship_address = Column("ship_address", String)
    ship_city = Column("ship_city", String)
    ship_region = Column("ship_region", String)
    ship_postal_code = Column("ship_postal_code", String)
    ship_country = Column("ship_country", String)
    order_details = relationship(
        "OrderDetails", back_populates="orders", cascade="all, delete-orphan"
    )


class OrderDetails(Base):
    __tablename__ = "order_details"
    order_id = Column(
        "order_id",
        Integer,
        ForeignKey("orders.order_id", ondelete="CASCADE"),
        primary_key=True,
    )
    product_id = Column("product_id", Integer, primary_key=True)
    unit_price = Column("unit_price", Float)
    quantity = Column("quantity", Integer)
    discount = Column("discount", Float)
    orders = relationship("Orders", back_populates="order_details")


Base.metadata.create_all(engine)

# populate tables with the extracted data
Session = sessionmaker(bind=engine)
session = Session()
orders = [Orders(**row_data) for _, row_data in orders_data.iterrows()]
order_details = [
    OrderDetails(**row_data) for _, row_data in order_details_data.iterrows()
]
print(f"Insertig data into {OUTPUT_DB_NAME} database.")
print(f"Inserting {len(orders)} orders and {len(order_details)} order_details.")
session.add_all(orders)
session.add_all(order_details)
session.commit()
print("Done.")
session.close()  # to prevent resource leakage
engine.dispose()  # to prevent resource leakage
