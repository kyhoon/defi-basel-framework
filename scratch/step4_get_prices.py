import os
import time
from random import randrange

import requests
from dotenv import load_dotenv
from joblib import Parallel, delayed
from models import Price, Token, Transfer, commit
from pony import orm
from tqdm import tqdm
from utils import tqdm_joblib
from web3 import Web3

load_dotenv()

web3 = Web3(Web3.HTTPProvider(os.getenv("WEB3_PROVIDER")))

# get the list of tokens
with orm.db_session:
    tokens = list(orm.select(t.address for t in Token))

# get the blocknumbers from transfers
with orm.db_session:
    blocks = set(orm.select(tx.block_number for tx in Transfer))
blocks = sorted(list(blocks))[180000:]


def fetch_prices(block_number):
    with orm.db_session:
        transfers = set(
            orm.select(tx.token for tx in Transfer if tx.block_number == block_number)
        )
    _tokens = [token for token in transfers if token in tokens]
    if len(_tokens) == 0:
        return

    # get prices from DefiLlama
    url = "https://coins.llama.fi/prices/historical/"
    timestamp = web3.eth.get_block(int(block_number)).timestamp
    token_string = ",".join([f"ethereum:{token}" for token in _tokens])

    retry = True
    count = 0
    while retry:
        count += 1
        if count > 5:
            return
        try:
            res = requests.get(url + f"{timestamp}/{token_string}")
            if res.status_code == 200:
                retry = False
            elif res.status_code == 429:
                time.sleep(randrange(5, 10))
        except:
            time.sleep(randrange(5, 10))

    coins = res.json()["coins"]

    with orm.db_session:
        for key, value in coins.items():
            address = key.split(":")[1]
            token = Token.get(address=address)
            try:
                Price(
                    token=token,
                    block_number=block_number,
                    price=str(value["price"]),
                )
                commit()
            except orm.core.CacheIndexError:
                continue


# parallel
jobs = [delayed(fetch_prices)(block) for block in blocks]
pool = Parallel(backend="threading", n_jobs=16)

with tqdm_joblib(tqdm(total=len(jobs))) as pbar:
    result = pool(jobs)
