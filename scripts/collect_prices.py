import json
import logging
import math
import time
from joblib import Parallel, delayed

import requests
from sqlalchemy.dialects.postgresql import insert
from tqdm import tqdm

from data.base import Session
from data.models import Price, Token
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
BATCH_SIZE = 50

MIN_TIMESTAMP = 1534377600  # 2018-Aug-16
INTERVAL = 60 * 60 * 24  # daily

url = "https://coins.llama.fi"


def query_defillama(endpoint):
    retries = 0
    while retries <= RETRY_MAX:
        try:
            resp = requests.get(url + endpoint)
            if resp.status_code == 200:
                return resp.json()
            else:
                raise resp.raise_for_status()
        except Exception as e:
            logger.debug(e)
            time.sleep(RETRY_BACKOFF * 2**retries)
            retries += 1
    logger.debug("could not fetch data from DefiLlama")
    return {}


def fetch_first_timestamps(tokens):
    token_strings = [f"ethereum:{token}" for token in tokens]
    out_dict = {}
    for idx in range(0, len(token_strings), BATCH_SIZE):
        _token_strings = ",".join(token_strings[idx : idx + BATCH_SIZE])
        data = query_defillama(f"/prices/first/{_token_strings}")
        data = data.get("coins", {})
        for key, value in data.items():
            timestamp = math.ceil(value["timestamp"] / INTERVAL) * INTERVAL
            out_dict[key] = timestamp
    return out_dict


def fetch_prices(token, first_timestamp, timestamp_to_block):
    session = Session()

    address = token.split(":")[-1]
    last_price = (
        session.query(Price)
        .filter(Price.token_id == address)
        .order_by(Price.timestamp.desc())
        .first()
    )
    if last_price is not None:
        first_timestamp = last_price.timestamp
    timestamps = list(filter(lambda ts: ts >= first_timestamp, timestamp_to_block))

    for idx in range(0, len(timestamps), BATCH_SIZE):
        _timestamps = timestamps[idx : idx + BATCH_SIZE]
        batch = json.dumps({token: _timestamps})
        data = query_defillama(f"/batchHistorical?coins={batch}")
        data = data.get("coins", {}).get(token, {}).get("prices", [])

        prices = []
        for _data in data:
            timestamp = _data["timestamp"] // INTERVAL * INTERVAL
            timestamp = max(timestamp, timestamps[0])
            prices.append(
                {
                    "block_number": timestamp_to_block[timestamp],
                    "token_id": address,
                    "timestamp": timestamp,
                    "usd_value": _data["price"],
                }
            )

        stmt = (
            insert(Price)
            .values(prices)
            .on_conflict_do_nothing(index_elements=["block_number", "token_id"])
        )
        session.execute(stmt)
        session.commit()

    session.close()


def run():
    session = Session()

    tokens = session.query(Token.id).all()
    tokens = list(token[0] for token in tokens)
    logger.info(f"collecting block numbers for {len(tokens)} tokens")
    logger.info(
        f"with the interval of {INTERVAL // 3600} hours {INTERVAL // 60 % 60} minutes {INTERVAL % 60} seconds"
    )

    session.close()

    first_ts = fetch_first_timestamps(tokens)
    min_ts = min(first_ts.values())
    min_ts = max(MIN_TIMESTAMP // INTERVAL * INTERVAL, min_ts)
    now = int(time.time() // INTERVAL * INTERVAL)

    ts_to_block = {}
    for ts in tqdm(range(min_ts, now, INTERVAL)):
        block_data = query_defillama(f"/block/ethereum/{ts}")
        block_number = block_data.get("height")
        if block_number is None:
            logger.debug(f"could not fetch block number for timestamp {ts}")
            continue
        ts_to_block[ts] = block_number

    logger.info(f"collecting prices for {len(ts_to_block)} blocks")

    # parallel
    pool = Parallel(backend="loky", n_jobs=N_JOBS)
    jobs = [
        delayed(fetch_prices)(key, value, ts_to_block)
        for key, value in first_ts.items()
    ]
    logger.info(f"using {N_JOBS} processes")

    with tqdm_joblib(tqdm(total=len(jobs))) as pbar:
        result = pool(jobs)
    # [fetch_prices(key, value, ts_to_block) for key, value in first_ts.items()]
