import hashlib
import logging
import os
import time

import requests
from sqlalchemy import delete
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import make_transient

from data.base import Session
from data.models import Token, Transfer, TransferSnapshot

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
RETRY_MAX = 5
RETRY_BACKOFF = 0.2


def query_etherscan(params):
    retries = 0
    while retries <= RETRY_MAX:
        try:
            resp = requests.get("https://api.etherscan.io/api", params=params)
            data = resp.json()
            if data["status"] == "1" or data["message"] == "No transactions found":
                return data["result"]
            else:
                raise ConnectionError(data["result"])
        except Exception as e:
            logger.debug(e)
            time.sleep(RETRY_BACKOFF * 2**retries)
            retries += 1
    raise ConnectionError("could not fetch data from Etherscan")


def get_block_number(timestamp):
    params = {
        "module": "block",
        "action": "getblocknobytime",
        "timestamp": timestamp,
        "closest": "before",
        "apikey": os.getenv("ETHERSCAN_TOKEN"),
    }
    return query_etherscan(params)


def get_transactions(address, from_block, to_block):
    params = {
        "module": "account",
        "action": "tokentx",
        "address": address,
        "offset": 10000,
        "startblock": from_block,
        "endblock": to_block,
        "sort": "asc",
        "apikey": os.getenv("ETHERSCAN_TOKEN"),
    }
    return query_etherscan(params)


def _collect_transfers():
    with Session() as session:
        tokens = session.query(Token.id).all()
        tokens = set(token[0] for token in tokens)
        snapshot = (
            session.query(TransferSnapshot)
            .order_by(
                TransferSnapshot.treasury_id,
                TransferSnapshot.from_timestamp,
                TransferSnapshot.to_timestamp,
            )
            .first()
        )
        # remove snapshot
        logger.info(f"collecting txs for snapshot {snapshot}")
        stmt = delete(TransferSnapshot).filter(
            TransferSnapshot.treasury_id == snapshot.treasury_id,
            TransferSnapshot.from_timestamp == snapshot.from_timestamp,
            TransferSnapshot.to_timestamp == snapshot.to_timestamp,
        )
        session.execute(stmt)
        session.commit()

    with Session() as session:
        # collect snapshot
        try:
            from_block = get_block_number(snapshot.from_timestamp)
            to_block = get_block_number(snapshot.to_timestamp)
            is_last_page = False
            while not is_last_page:
                data = get_transactions(snapshot.treasury_id, from_block, to_block)
                is_last_page = len(data) < 10000
                if not is_last_page:
                    from_block = data[-1]["blockNumber"]

                txs = [
                    {
                        "block_hash": tx["blockHash"],
                        "tx_hash": tx["hash"],
                        "log_index": tx["transactionIndex"],
                        "timestamp": tx["timeStamp"],
                        "block_number": int(tx["blockNumber"]),
                        "token_id": tx["contractAddress"],
                        "from_address": tx["from"],
                        "to_address": tx["to"],
                        "value": tx["value"],
                    }
                    for tx in data
                    if tx["contractAddress"] in tokens
                ]
                for tx in txs:
                    tx_str = str(tx).encode("utf-8")
                    tx["id"] = hashlib.md5(tx_str).hexdigest()
                    del tx["block_hash"]
                    del tx["tx_hash"]
                    del tx["log_index"]

                if len(txs) == 0:
                    continue

                logger.debug(f"collecting {len(txs)} txs for snapshot {snapshot}")
                stmt = (
                    insert(Transfer)
                    .values(txs)
                    .on_conflict_do_nothing(index_elements=["id"])
                )
                session.execute(stmt)
                session.commit()

        # add snapshot back in
        except ConnectionError:
            make_transient(snapshot)
            session.add(snapshot)
            session.commit()
            logger.error(f"skipping snapshot {snapshot} due to connection error")


def collect_transfers():
    with Session() as session:
        rows = session.query(TransferSnapshot).count()
    if rows == 0:
        return

    _collect_transfers()
