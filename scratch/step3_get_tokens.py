"""classify tokens into security vs. utility"""
import os
import time

import pandas as pd
import requests

DATA_DIR = "../data"
BATCH_SIZE = 30

# get token addresses
tokens = set({})
for filename in os.listdir(os.path.join(DATA_DIR, "transfers")):
    df = pd.read_csv(os.path.join(DATA_DIR, "transfers", filename), index_col=0)
    if len(df) == 0:
        continue
    tokens.update(df.address)
tokens = list(tokens)

# get prices from DefiLlama
url = "https://coins.llama.fi/prices/current/"

tokens_llama = []
for idx in range(0, len(tokens), BATCH_SIZE):
    _tokens = tokens[idx : idx + BATCH_SIZE]
    token_string = ",".join([f"ethereum:{_token}" for _token in _tokens])
    res = requests.get(url + token_string)
    coins = res.json()["coins"]
    tokens_llama.extend([coin.split(":")[1].lower() for coin in coins.keys()])

# get prices from coingecko
url = "https://api.coingecko.com/api/v3/simple/token_price/ethereum"

tokens_gecko = []
for idx in range(0, len(tokens_llama), BATCH_SIZE):
    _tokens = tokens_llama[idx : idx + BATCH_SIZE]
    res = requests.get(
        url, params={"vs_currencies": "usd", "contract_addresses": ",".join(_tokens)}
    )
    tokens_gecko.extend(
        [
            key.lower()
            for key, value in res.json().items()
            if (len(value) > 0 and key.startswith("0x"))
        ]
    )
    time.sleep(3)

# security tokens
security_tokens = set(tokens_gecko)
for token in security_tokens:
    df = pd.DataFrame(columns=["block_number", "symbol", "decimals", "price"])
    df.to_csv(os.path.join(DATA_DIR, "tokens", "security", token + ".csv"))

# utility tokens
utility_tokens = set(tokens_llama).difference(security_tokens)
for token in utility_tokens:
    df = pd.DataFrame(columns=["block_number", "symbol", "decimals", "price"])
    df.to_csv(os.path.join(DATA_DIR, "tokens", "utility", token + ".csv"))
