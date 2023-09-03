import json
import logging
import os
import time

from joblib import Parallel, delayed
from sqlalchemy.dialects.postgresql import insert

from data.base import Session
from data.models import (
    Price,
    PriceSnapshot,
    Protocol,
    Token,
    Transfer,
    TransferSnapshot,
    Treasury,
)

# logger
logger = logging.getLogger(__file__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter(
    "%(asctime)s - %(levelname)s - %(filename)s:%(lineno)s - %(message)s"
)
sh = logging.StreamHandler()
sh.setFormatter(formatter)
logger.addHandler(sh)

# config
MIN_TIMESTAMP = 1534377600  # 2018-Aug-16
INTERVAL = 60 * 60 * 24  # daily
OFFSET = 100000


def update_protocols():
    logger.debug("updating protocols from JSON")

    with Session() as session:
        protocols = os.listdir("data/protocols")
        logger.debug(f"found {len(protocols)} protocols")
        for filename in protocols:
            with open(f"data/protocols/{filename}") as f:
                data = json.load(f)

            # update protocol info
            protocol_id = filename.split(".")[0]
            addresses = list(
                set(addr.lower() for addr in data["treasury"] + data["addresses"])
            )
            stmt = (
                insert(Protocol)
                .values(
                    id=protocol_id,
                    rating=data["rating"],
                    addresses=addresses,
                    hacks=data["hacks"],
                )
                .on_conflict_do_update(
                    index_elements=["id"],
                    set_={
                        "rating": data["rating"],
                        "addresses": addresses,
                        "hacks": data["hacks"],
                    },
                )
            )
            session.execute(stmt)
            session.commit()

            # update treasuries
            if len(data["treasury"]) == 0:
                continue
            treasuries = set(addr.lower() for addr in data["treasury"])
            treasuries = [
                {
                    "id": addr,
                    "protocol_id": protocol_id,
                }
                for addr in treasuries
            ]
            stmt = (
                insert(Treasury)
                .values(treasuries)
                .on_conflict_do_update(
                    index_elements=["id"],
                    set_={"protocol_id": protocol_id},
                )
            )
            session.execute(stmt)
            session.commit()

    logger.debug("updating protocols complete")


def update_tokens():
    logger.debug("updating tokens from JSON")

    with Session() as session:
        tokens = os.listdir("data/tokens")
        logger.debug(f"found {len(tokens)} tokens")
        for filename in tokens:
            with open(f"data/tokens/{filename}") as f:
                data = json.load(f)

            # update token info
            if data["underlying"] is not None:
                data["underlying"] = data["underlying"].lower()
            stmt = (
                insert(Token)
                .values(
                    id=filename.split(".")[0].lower(),
                    protocol_id=data["protocol"],
                    symbol=data["symbol"],
                    itin=data["itin"],
                    decimals=data["decimals"],
                    itc_eep=data["itc_eep"],
                    underlying=data["underlying"],
                )
                .on_conflict_do_update(
                    index_elements=["id"],
                    set_={
                        "protocol_id": data["protocol"],
                        "symbol": data["symbol"],
                        "itin": data["itin"],
                        "decimals": data["decimals"],
                        "itc_eep": data["itc_eep"],
                        "underlying": data["underlying"],
                    },
                )
            )
            session.execute(stmt)
            session.commit()

    logger.debug("updating tokens complete")


def create_timestamps():
    logger.debug(
        f"creating tracks with the interval of {INTERVAL // 3600} hours {INTERVAL // 60 % 60} minutes {INTERVAL % 60} seconds"
    )
    min_ts = MIN_TIMESTAMP // INTERVAL * INTERVAL
    now = int(time.time() // INTERVAL * INTERVAL)
    timestamps = list(range(min_ts, now, INTERVAL))
    return timestamps


def check_transfers(timestamps):
    logger.debug(f"checking transfers for {len(timestamps)} timestamps")

    with Session() as session:
        treasuries = session.query(Treasury).all()
        idx = 1
        snapshots = []
        while idx < len(timestamps):
            start = timestamps[idx - 1]
            end = timestamps[idx]
            idx += 1

            tx_ts = session.query(Transfer).filter(
                Transfer.timestamp >= start, Transfer.timestamp < end
            )
            for treasury in treasuries:
                tx_cnt = tx_ts.filter(
                    (Transfer.from_address == treasury.id)
                    | (Transfer.to_address == treasury.id)
                ).first()
                if tx_cnt is None:
                    snapshots.append(
                        {
                            "treasury_id": treasury.id,
                            "from_timestamp": start,
                            "to_timestamp": end,
                        }
                    )
            if len(snapshots) > OFFSET:
                logger.debug(
                    f"adding {len(snapshots)} snapshots for transfers {idx}/{len(timestamps) - 1}"
                )
                stmt = (
                    insert(TransferSnapshot)
                    .values(snapshots)
                    .on_conflict_do_nothing(
                        index_elements=["treasury_id", "from_timestamp", "to_timestamp"]
                    )
                )
                session.execute(stmt)
                session.commit()
                snapshots = []

        if len(snapshots) > 0:
            logger.debug(
                f"adding {len(snapshots)} snapshots for transfers {len(timestamps) - 1}/{len(timestamps) - 1}"
            )
            stmt = (
                insert(TransferSnapshot)
                .values(snapshots)
                .on_conflict_do_nothing(
                    index_elements=["treasury_id", "from_timestamp", "to_timestamp"]
                )
            )
            session.execute(stmt)
            session.commit()

    logger.debug("checking transfers complete")


def check_prices(timestamps):
    logger.debug(f"checking prices for {len(timestamps)} timestamps")

    with Session() as session:
        tokens = session.query(Token).all()
        idx = 1
        snapshots = []
        while idx < len(timestamps):
            ts = timestamps[idx]
            idx += 1

            prices_ts = session.query(Price).filter(Price.timestamp == ts)
            for token in tokens:
                prices_token = prices_ts.filter(Price.token_id == token.id).first()
                if prices_token is None:
                    snapshots.append(
                        {
                            "token_id": token.id,
                            "timestamp": ts,
                        }
                    )
            if len(snapshots) > OFFSET:
                logger.debug(
                    f"adding {len(snapshots)} snapshots for prices {idx}/{len(timestamps) - 1}"
                )
                stmt = (
                    insert(PriceSnapshot)
                    .values(snapshots)
                    .on_conflict_do_nothing(index_elements=["token_id", "timestamp"])
                )
                session.execute(stmt)
                session.commit()
                snapshots = []

        if len(snapshots) > 0:
            logger.debug(
                f"adding {len(snapshots)} snapshots for prices {len(timestamps) - 1}/{len(timestamps) - 1}"
            )
            stmt = (
                insert(PriceSnapshot)
                .values(snapshots)
                .on_conflict_do_nothing(index_elements=["token_id", "timestamp"])
            )
            session.execute(stmt)
            session.commit()

    logger.debug("checking prices complete")


def init_transfers(timestamps):
    logger.debug(f"initializing transfers for {len(timestamps)} timestamps")

    with Session() as session:
        treasuries = session.query(Treasury).all()

        snapshots = [
            {
                "treasury_id": treasury.id,
                "from_timestamp": timestamps[0],
                "to_timestamp": timestamps[-1],
            }
            for treasury in treasuries
        ]

        logger.debug(
            f"adding {len(snapshots)} snapshots for transfers {len(timestamps) - 1}/{len(timestamps) - 1}"
        )
        stmt = (
            insert(TransferSnapshot)
            .values(snapshots)
            .on_conflict_do_nothing(
                index_elements=["treasury_id", "from_timestamp", "to_timestamp"]
            )
        )
        session.execute(stmt)
        session.commit()


def init_prices(timestamps):
    logger.debug(f"initializing prices for {len(timestamps)} timestamps")

    with Session() as session:
        tokens = session.query(Token).all()
        idx = 1
        snapshots = []
        while idx < len(timestamps):
            ts = timestamps[idx]
            idx += 1

            snapshots.extend(
                [
                    {
                        "token_id": token.id,
                        "timestamp": ts,
                    }
                    for token in tokens
                ]
            )
            if len(snapshots) > OFFSET:
                logger.debug(
                    f"adding {len(snapshots)} snapshots for prices {idx}/{len(timestamps) - 1}"
                )
                stmt = (
                    insert(PriceSnapshot)
                    .values(snapshots)
                    .on_conflict_do_nothing(index_elements=["token_id", "timestamp"])
                )
                session.execute(stmt)
                session.commit()
                snapshots = []

        if len(snapshots) > 0:
            logger.debug(
                f"adding {len(snapshots)} snapshots for prices {len(timestamps) - 1}/{len(timestamps) - 1}"
            )
            stmt = (
                insert(PriceSnapshot)
                .values(snapshots)
                .on_conflict_do_nothing(index_elements=["token_id", "timestamp"])
            )
            session.execute(stmt)
            session.commit()


def initialize_snapshots():
    logger.info("initializing snapshots from JSON")

    update_protocols()
    update_tokens()
    timestamps = create_timestamps()

    Parallel(backend="loky", n_jobs=2)(
        [
            delayed(init_transfers)(timestamps),
            delayed(init_prices)(timestamps),
        ]
    )
    logger.info("initializing snapshots complete")


def update_snapshots():
    logger.info("updating snapshots from JSON")

    update_protocols()
    update_tokens()
    timestamps = create_timestamps()

    Parallel(backend="loky", n_jobs=2)(
        [
            delayed(check_transfers)(timestamps),
            delayed(check_prices)(timestamps),
        ]
    )
    logger.info("updating snapshots complete")
