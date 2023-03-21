"""get yearn addresses"""
import requests
from models import Contract, Protocol, commit
from pony import orm

url = "https://ydaemon.yearn.finance/1/vaults/all"
res = requests.get(url)
vaults = [vault["address"] for vault in res.json()]

url = "https://ydaemon.yearn.finance/1/strategies/all"
res = requests.get(url)
strategies = [strat["address"] for strat in res.json()]

addresses = vaults + strategies
addresses = list(set(addresses))
treasury = "0xFEB4acf3df3cDEA7399794D0869ef76A6EfAff52"

with orm.db_session:
    # create Yearn protocol
    protocol = Protocol(name="Yearn")
    commit()

    # add contracts
    for address in addresses:
        protocol = Protocol.get(name="Yearn")
        contract = Contract(address=address, protocol=protocol)
        commit()

    # add treasury address
    protocol = Protocol.get(name="Yearn")
    treasury = Contract(address=treasury, protocol=protocol)
    protocol.treasury = treasury
    commit()
