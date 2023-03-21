"""classify tokens into security vs. utility"""
import time

import requests
from models import Token, Transfer, commit
from pony import orm
from web3 import Web3

BATCH_SIZE = 30

# get token addresses
with orm.db_session:
    tokens = set(orm.select(tx.token for tx in Transfer))
tokens = list(tokens)

# get prices from DefiLlama
url = "https://coins.llama.fi/prices/current/"

tokens_llama = []
decimals = {}
for idx in range(0, len(tokens), BATCH_SIZE):
    _tokens = tokens[idx : idx + BATCH_SIZE]
    token_string = ",".join([f"ethereum:{_token}" for _token in _tokens])
    res = requests.get(url + token_string)
    coins = res.json()["coins"]
    for key, value in coins.items():
        address = key.split(":")[1].lower()
        tokens_llama.append(address)
        decimals[address] = value["decimals"]

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
with orm.db_session:
    for token in security_tokens:
        Token(
            address=Web3.toChecksumAddress(token),
            category="security",
            decimals=decimals[token],
        )
        commit()

# utility tokens
utility_tokens = set(tokens_llama).difference(security_tokens)
with orm.db_session:
    for token in utility_tokens:
        Token(
            address=Web3.toChecksumAddress(token),
            category="utility",
            decimals=decimals[token],
        )
        commit()
