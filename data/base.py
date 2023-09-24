import os

import numpy as np
from psycopg2.extensions import AsIs, register_adapter
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

username = os.getenv("POSTGRES_USER")
password = os.getenv("POSTGRES_PASSWORD")
host = os.getenv("POSTGRES_HOST")
port = os.getenv("POSTGRES_PORT")
database = os.getenv("POSTGRES_DB")

conn_str = f"postgresql://{username}:{password}@{host}:{port}/{database}"
engine = create_engine(
    conn_str,
    pool_size=0,
    pool_pre_ping=True,
    connect_args={
        "keepalives": 1,
        "keepalives_idle": 30,
        "keepalives_interval": 10,
        "keepalives_count": 5,
    },
)
Session = sessionmaker(bind=engine)

Base = declarative_base()

register_adapter(np.int64, AsIs)
register_adapter(np.float64, AsIs)
