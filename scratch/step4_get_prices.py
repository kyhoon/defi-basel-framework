import contextlib
import os
import time
from random import randrange

import joblib
import pandas as pd
import requests
from joblib import Parallel, delayed
from tqdm import tqdm
from web3 import Web3

DATA_DIR = "../data"
web3 = Web3(Web3.HTTPProvider("http://localhost:8547"))


@contextlib.contextmanager
def tqdm_joblib(tqdm_object):
    """Context manager to patch joblib to report into tqdm progress bar given as argument"""

    class TqdmBatchCompletionCallback(joblib.parallel.BatchCompletionCallBack):
        def __call__(self, *args, **kwargs):
            tqdm_object.update(n=self.batch_size)
            return super().__call__(*args, **kwargs)

    old_batch_callback = joblib.parallel.BatchCompletionCallBack
    joblib.parallel.BatchCompletionCallBack = TqdmBatchCompletionCallback
    try:
        yield tqdm_object
    finally:
        joblib.parallel.BatchCompletionCallBack = old_batch_callback
        tqdm_object.close()


# get the list of tokens
security_tokens = os.listdir(os.path.join(DATA_DIR, "tokens", "security"))
security_tokens = [token.split(".csv")[0] for token in set(security_tokens)]

utility_tokens = os.listdir(os.path.join(DATA_DIR, "tokens", "utility"))
utility_tokens = [token.split(".csv")[0] for token in set(utility_tokens)]

tokens = {token: set({}) for token in security_tokens + utility_tokens}

# get the blocknumbers from transfers
root = os.path.join(DATA_DIR, "transfers")
for filename in os.listdir(root):
    df = pd.read_csv(os.path.join(root, filename), index_col=0)
    if len(df) == 0:
        continue

    df_tokens = set(df.address.str.lower())
    for _token in df_tokens:
        if _token not in tokens:
            continue
        tokens[_token].update(df.blockNumber[df.address.str.lower() == _token].values)

block_numbers = set([item for sublist in tokens.values() for item in sublist])
block_numbers = {block: set({}) for block in block_numbers}
for token, blocks in tokens.items():
    for block in blocks:
        block_numbers[block].add(token)


def fetch_prices(block_number, tokens):
    # get prices from DefiLlama
    url = "https://coins.llama.fi/prices/historical/"
    timestamp = web3.eth.get_block(int(block_number)).timestamp
    token_string = ",".join([f"ethereum:{token}" for token in tokens])

    retry = True
    while retry:
        try:
            res = requests.get(url + f"{timestamp}/{token_string}")
        except:
            time.sleep(randrange(3, 10))
            continue

        if res.status_code == 200:
            retry = False
        else:
            time.sleep(randrange(3, 10))
    coins = res.json()["coins"]

    # save in csv
    for key, value in coins.items():
        token = key.split(":")[1]
        if token in security_tokens:
            path = os.path.join(DATA_DIR, "tokens", "security", token + ".csv")
        elif token in utility_tokens:
            path = os.path.join(DATA_DIR, "tokens", "utility", token + ".csv")
        else:
            raise ValueError("this should not happen")

        try:
            df = pd.read_csv(path, index_col=0)
        except:
            continue
        value["block_number"] = block_number
        df = df.append(value, ignore_index=True)
        df.to_csv(path)


# parallel
jobs = [delayed(fetch_prices)(block, tokens) for block, tokens in block_numbers.items()]
pool = Parallel(backend="threading", n_jobs=16)

with tqdm_joblib(tqdm(total=len(jobs))) as pbar:
    result = pool(jobs)
