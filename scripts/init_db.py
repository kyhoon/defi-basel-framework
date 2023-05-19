import json
import logging
import os

from sqlalchemy.dialects.postgresql import insert
from tqdm import tqdm

from data.base import Base, Session, engine
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

        stmt = (
            insert(Protocol)
            .values(
                id=filename.split(".")[0],
                rating=data["rating"],
                treasury=[address.lower() for address in data["treasury"]],
                addresses=[address.lower() for address in data["addresses"]],
            )
            .on_conflict_do_update(
                index_elements=["id"],
                set_={
                    "rating": data["rating"],
                    "treasury": [address.lower() for address in data["treasury"]],
                    "addresses": [address.lower() for address in data["addresses"]],
                },
            )
        )
        session.execute(stmt)
        session.commit()

    # populate tokens
    logger.info(f"populating {len(os.listdir('data/tokens'))} tokens")
    for filename in tqdm(os.listdir("data/tokens")):
        with open(f"data/tokens/{filename}") as f:
            data = json.load(f)

        stmt = (
            insert(Token)
            .values(
                id=filename.split(".")[0].lower(),
                protocol_id=data["protocol"],
                symbol=data["symbol"],
                itin=data["itin"],
                decimals=data["decimals"],
            )
            .on_conflict_do_update(
                index_elements=["id"],
                set_={
                    "protocol_id": data["protocol"],
                    "symbol": data["symbol"],
                    "itin": data["itin"],
                    "decimals": data["decimals"],
                },
            )
        )
        session.execute(stmt)
        session.commit()

    session.close()
