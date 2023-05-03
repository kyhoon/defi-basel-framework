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
engine = create_engine(conn_str)
Session = sessionmaker(bind=engine)

Base = declarative_base()
