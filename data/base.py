import os
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from dotenv import load_dotenv


load_dotenv()

username = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
host = os.getenv("DB_HOST")
port = 5432
database = os.getenv("DB_DATABASE")

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
