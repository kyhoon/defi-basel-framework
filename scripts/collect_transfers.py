import logging
import os
import time
from joblib import Parallel, delayed

import requests
from dotenv import load_dotenv
from sqlalchemy.dialects.postgresql import insert
from tqdm import tqdm

from data.base import Session
from data.models import Protocol, Token, Transfer
from scripts.utils import tqdm_joblib

# logger
logger = logging.getLogger(__file__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter(
    "%(asctime)s - %(process)d - %(levelname)s - %(filename)s:%(lineno)s - %(message)s"
)
sh = logging.StreamHandler()
sh.setFormatter(formatter)
logger.addHandler(sh)

# config
N_JOBS = 4
RETRY_MAX = 5
RETRY_BACKOFF = 0.2
OFFSET = 10000

load_dotenv()

url = "https://api.etherscan.io/api"
params = {
    "module": "account",
    "action": "tokentx",
    "offset": OFFSET,
    "sort": "asc",
    "startblock": 0,
    "endblock": 99999999,
    "apikey": os.getenv("ETHERSCAN_TOKEN"),
}


def query_etherscan(params):
    retries = 0
    while retries <= RETRY_MAX:
        try:
            resp = requests.get(url, params=params)
            data = resp.json()
            if data["status"] == "1":
                return data["result"]
            else:
                raise ConnectionError(data["result"])
        except Exception as e:
            logger.debug(e)
            time.sleep(RETRY_BACKOFF * 2**retries)
            retries += 1
    logger.debug("could not fetch data from Etherscan")
    return []


def fetch_transfers(address, tokens):
    session = Session()

    _params = params.copy()
    _params["address"] = address

    last_tx = (
        session.query(Transfer)
        .filter((Transfer.from_address == address) | (Transfer.to_address == address))
        .order_by(Transfer.block_number.desc())
        .first()
    )
    if last_tx is not None:
        _params["startblock"] = last_tx.block_number

    page = 0
    data = []
    is_last_page = False
    while not is_last_page:
        page += 1
        if page * OFFSET > 10000:
            page = 0
            _params["startblock"] = data[-1]["blockNumber"]
        _params["page"] = str(page)

        data = query_etherscan(_params)
        is_last_page = len(data) < OFFSET

        txs = [
            {
                "block_hash": tx["blockHash"],
                "tx_hash": tx["hash"],
                "log_index": tx["transactionIndex"],
                "block_number": int(tx["blockNumber"]),
                "token_id": tx["contractAddress"],
                "from_address": tx["from"],
                "to_address": tx["to"],
                "value": tx["value"],
            }
            for tx in data
            if tx["contractAddress"] in tokens
        ]
        if len(txs) == 0:
            continue

        stmt = (
            insert(Transfer)
            .values(txs)
            .on_conflict_do_nothing(
                index_elements=["block_hash", "tx_hash", "log_index"]
            )
        )
        session.execute(stmt)
        session.commit()

    session.close()


def run():
    session = Session()

    addresses = set({})
    for protocol in session.query(Protocol).all():
        addresses.update(protocol.treasury)
        addresses.update(protocol.addresses)
    addresses = set(address.lower() for address in addresses)
    logger.info(f"collecting transfers for {len(addresses)} addresses")

    tokens = session.query(Token.id).all()
    tokens = set(token[0] for token in tokens)
    logger.info(f"filtering transfers by {len(tokens)} tokens")

    session.close()

    # parallel
    pool = Parallel(backend="loky", n_jobs=N_JOBS)
    jobs = [delayed(fetch_transfers)(address, tokens) for address in addresses]
    logger.info(f"using {N_JOBS} processes")

    with tqdm_joblib(tqdm(total=len(jobs))) as pbar:
        result = pool(jobs)
