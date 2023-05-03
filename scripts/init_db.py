import os
import json
from tqdm import tqdm
from sqlalchemy.dialects.postgresql import insert
import logging

from data.base import Base, engine, Session
from data.models import Protocol, Token

# logger
logger = logging.getLogger(__file__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter(
    "%(asctime)s - %(process)d - %(levelname)s - %(filename)s:%(lineno)s - %(message)s"
)
sh = logging.StreamHandler()
sh.setFormatter(formatter)
logger.addHandler(sh)


def run():
    Base.metadata.create_all(engine)
    session = Session()

    # populate protocols
    logger.info(f"populating {len(os.listdir('data/protocols'))} protocols")
    for filename in tqdm(os.listdir("data/protocols")):
        with open(f"data/protocols/{filename}") as f:
            data = json.load(f)

        statement = (
            insert(Protocol)
            .values(
                id=filename.split(".")[0],
                rating=data["rating"],
                treasury=data["treasury"],
                addresses=data["addresses"],
            )
            .on_conflict_do_nothing(index_elements=["id"])
        )
        session.execute(statement)
        session.commit()

    # populate tokens
    logger.info(f"populating {len(os.listdir('data/tokens'))} tokens")
    for filename in tqdm(os.listdir("data/tokens")):
        with open(f"data/tokens/{filename}") as f:
            data = json.load(f)

        statement = (
            insert(Token)
            .values(
                id=filename.split(".")[0],
                protocol_id=data["protocol"],
                symbol=data["symbol"],
                itin=data["itin"],
                decimals=data["decimals"],
            )
            .on_conflict_do_nothing(index_elements=["id"])
        )
        session.execute(statement)
        session.commit()

    session.close()
