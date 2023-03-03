"""get yearn addresses"""
import json
import os

import requests

DATA_DIR = "../data"

url = "https://ydaemon.yearn.finance/1/vaults/all"
res = requests.get(url)
vaults = [vault["address"] for vault in res.json()]

url = "https://ydaemon.yearn.finance/1/strategies/all"
res = requests.get(url)
strategies = [strat["address"] for strat in res.json()]

addresses = vaults + strategies
addresses = list(set(addresses))
addresses = {
    "treasury": "0xfeb4acf3df3cdea7399794d0869ef76a6efaff52",
    "addresses": addresses,
}

with open(os.path.join(DATA_DIR, "addresses.json"), "w") as f:
    json.dump(addresses, f)
