import json
import logging
import time
from joblib import Parallel, delayed

import requests
from sqlalchemy import delete
from sqlalchemy.dialects.postgresql import insert

from data.base import Session
from data.models import Price, PriceSnapshot

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
OFFSET = 50


def query_defillama(endpoint):
    retries = 0
    while retries <= RETRY_MAX:
        try:
            resp = requests.get("https://coins.llama.fi" + endpoint)
            if resp.status_code == 200:
                return resp.json()
            raise resp.raise_for_status()
        except Exception as e:
            logger.debug(e)
            time.sleep(RETRY_BACKOFF * 2**retries)
            retries += 1
    raise ConnectionError("could not fetch data from DefiLlama")


def _collect_prices(page):
    with Session() as session:
        snapshots = (
            session.query(PriceSnapshot)
            .order_by(PriceSnapshot.token_id, PriceSnapshot.timestamp)
            .offset(page * OFFSET)
            .limit(OFFSET)
            .all()
        )
    logger.info(f"collecting prices for snapshots {snapshots[0]}-{snapshots[-1]}")

    with Session() as session:
        # collect snapshots
        try:
            query_batch = {}
            for snapshot in snapshots:
                key = f"ethereum:{snapshot.token_id}"
                if key in query_batch:
                    query_batch[key].append(snapshot.timestamp)
                else:
                    query_batch[key] = [snapshot.timestamp]

            query_json = json.dumps(query_batch)
            data = query_defillama(f"/batchHistorical?coins={query_json}")

            prices = []
            for key, value in data.get("coins", {}).items():
                token_id = key.split(":")[-1]
                _prices = value.get("prices", [])
                prices.extend(
                    [
                        {
                            "token_id": token_id,
                            "timestamp": _price["timestamp"],
                            "value": _price["price"],
                        }
                        for _price in _prices
                    ]
                )

            if len(prices) > 0:
                stmt = (
                    insert(Price)
                    .values(prices)
                    .on_conflict_do_nothing(index_elements=["token_id", "timestamp"])
                )
                session.execute(stmt)
                session.commit()

        except ConnectionError:
            logger.error(
                f"skipping snapshots {snapshots[0]}-{snapshots[-1]} due to connection error"
            )
            return

        # remove snapshots
        for snapshot in snapshots:
            stmt = delete(PriceSnapshot).filter(
                PriceSnapshot.token_id == snapshot.token_id,
                PriceSnapshot.timestamp == snapshot.timestamp,
            )
            session.execute(stmt)
            session.commit()


def collect_prices():
    with Session() as session:
        rows = session.query(PriceSnapshot).count()
    if rows == 0:
        return

    pages = rows // OFFSET + (rows % OFFSET > 0)
    pages = min(pages, 8)
    Parallel(backend="loky", n_jobs=pages)(
        [delayed(_collect_prices)(page) for page in range(pages)]
    )
