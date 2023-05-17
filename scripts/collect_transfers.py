import time
import pickle
import gzip
import gc
import logging
import contextlib
import itertools
import json
import os
import hashlib

import requests
from sqlalchemy.dialects.postgresql import insert
from dotenv import load_dotenv
from eth_utils import to_bytes
from hexbytes import HexBytes
import joblib
from joblib import Parallel, delayed
from tqdm import tqdm
from web3 import Web3
from web3._utils.abi import (
    exclude_indexed_event_inputs,
    get_abi_input_names,
    get_indexed_event_inputs,
    map_abi_data,
    normalize_event_input_types,
)
from web3._utils.encoding import hexstr_if_str
from web3._utils.events import get_event_abi_types_for_decoding
from web3._utils.filters import construct_event_filter_params
from web3._utils.normalizers import BASE_RETURN_NORMALIZERS
from web3.types import ABIEvent

from data.base import Session
from data.models import Protocol, Transfer, Contract, Token

# logger
logger = logging.getLogger(__file__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter(
    "%(asctime)s - %(process)d - %(levelname)s - %(filename)s:%(lineno)s - %(message)s"
)
sh = logging.StreamHandler()
sh.setFormatter(formatter)
logger.addHandler(sh)

# config
BLOCK_SIZE = 10000
N_JOBS = 50

load_dotenv()
web3 = Web3(
    Web3.HTTPProvider(os.getenv("WEB3_PROVIDER"), request_kwargs={"timeout": 60})
)

# minimal ABI for ERC20 transfer event
ABI = """[
    {
        "anonymous": false,
        "inputs": [
            {
                "indexed": true,
                "name": "from",
                "type": "address"
            },
            {
                "indexed": true,
                "name": "to",
                "type": "address"
            },
            {
                "indexed": false,
                "name": "value",
                "type": "uint256"
            }
        ],
        "name": "Transfer",
        "type": "event"
    }
]
"""
abi = json.loads(ABI)
ERC20 = web3.eth.contract(abi=abi)


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


def parse_event(abi_codec, event_abi, log_entry):
    """modified from web3._utils.events.get_events"""
    if event_abi["anonymous"]:
        log_topics = log_entry["topics"]
    else:
        log_topics = log_entry["topics"][1:]

    log_topics_abi = get_indexed_event_inputs(event_abi)
    log_topic_normalized_inputs = normalize_event_input_types(log_topics_abi)
    log_topic_types = get_event_abi_types_for_decoding(log_topic_normalized_inputs)
    log_topic_names = get_abi_input_names(ABIEvent({"inputs": log_topics_abi}))

    log_data = hexstr_if_str(to_bytes, log_entry["data"])
    log_data_abi = exclude_indexed_event_inputs(event_abi)
    log_data_normalized_inputs = normalize_event_input_types(log_data_abi)
    log_data_types = get_event_abi_types_for_decoding(log_data_normalized_inputs)
    log_data_names = get_abi_input_names(ABIEvent({"inputs": log_data_abi}))

    decoded_log_data = abi_codec.decode(log_data_types, HexBytes(log_data))
    normalized_log_data = map_abi_data(
        BASE_RETURN_NORMALIZERS, log_data_types, decoded_log_data
    )

    decoded_topic_data = [
        abi_codec.decode([topic_type], topic_data)[0]
        for topic_type, topic_data in zip(log_topic_types, log_topics)
    ]
    normalized_topic_data = map_abi_data(
        BASE_RETURN_NORMALIZERS, log_topic_types, decoded_topic_data
    )

    event_args = dict(
        itertools.chain(
            zip(log_topic_names, normalized_topic_data),
            zip(log_data_names, normalized_log_data),
        )
    )
    event_data = {
        "block_hash": log_entry["blockHash"].hex(),
        "tx_hash": log_entry["transactionHash"].hex(),
        "log_index": log_entry["logIndex"],
        "block_number": log_entry["blockNumber"],
        "token_id": log_entry["address"],
        "from_address": event_args["from"],
        "to_address": event_args["to"],
        "value": str(event_args["value"]),
    }
    return event_data


def fetch_transfers(address, to_block, path):
    st0 = time.time()

    # get from block
    session = Session()
    contract = session.get(Contract, address)
    from_block = contract.from_block

    event = ERC20.events.Transfer
    event_abi = event._get_event_abi()
    event_abi_codec = event.web3.codec

    for _from_block in range(from_block, to_block, BLOCK_SIZE):
        st1 = time.time()

        _to_block = min(to_block, _from_block + BLOCK_SIZE)

        _events = []

        # from address
        _, event_filter_params = construct_event_filter_params(
            event_abi,
            event_abi_codec,
            argument_filters={"from": address.lower()},
            fromBlock=_from_block,
            toBlock=_to_block,
        )

        st2 = time.time()

        patience = 1
        while True:
            try:
                logs = event.web3.eth.get_logs(event_filter_params)
                break
            except Exception as e:
                print(e)
                time.sleep(patience)
                patience *= 2
                continue

        for entry in logs:
            try:
                log = parse_event(event_abi_codec, event_abi, entry)
                token = session.get(Token, log["token_id"])
                if token is None:
                    continue
                _events.append(log)
            except Exception as e:
                print(e)
                continue

        # to address
        _, event_filter_params = construct_event_filter_params(
            event_abi,
            event_abi_codec,
            argument_filters={"to": address.lower()},
            fromBlock=_from_block,
            toBlock=_to_block,
        )

        patience = 1
        while True:
            try:
                logs = event.web3.eth.get_logs(event_filter_params)
                break
            except Exception as e:
                print(e)
                time.sleep(patience)
                patience *= 2
                continue

        for entry in logs:
            try:
                log = parse_event(event_abi_codec, event_abi, entry)
                token = session.get(Token, log["token_id"])
                if token is None:
                    continue
                _events.append(log)
            except Exception as e:
                print(e)
                continue

        print(f"time for get_logs: {time.time() - st2}")

        if len(_events) > 0:
            st3 = time.time()
            stmt = insert(Transfer).values(_events)
            try:
                session.execute(stmt)
                session.commit()
            except Exception as e:
                print(e)
                session.rollback()
            print(f"time for sql stmt: {time.time() - st3}")

        # update last block
        contract = session.get(Contract, address)
        contract.from_block = _to_block
        session.add(contract)
        session.commit()

        print(f"time for block: {time.time() - st1}")

    session.close()
    print(f"time for address: {time.time() - st0}")


def create_contracts():
    session = Session()
    addresses = set({})
    for protocol in session.query(Protocol).all():
        addresses.update(protocol.treasury)
        addresses.update(protocol.addresses)
    addresses = set(address.lower() for address in addresses)
    session.close()
    logger.info(f"found {len(addresses)} addresses in database")

    logger.info(f"populating contracts in database")
    session = Session()
    new_contracts = []
    for address in tqdm(addresses):
        contract = session.get(Contract, address)
        if contract is None:
            contract = Contract(id=address)
            session.add(contract)
            session.commit()
        if contract.from_block is None:
            new_contracts.append(address)
    session.close()
    logger.info(f"collecting creation blocks for {len(new_contracts)} addresses")

    url = "https://api.etherscan.io/api"
    params = {
        "module": "contract",
        "action": "getcontractcreation",
        "apikey": os.getenv("ETHERSCAN_TOKEN"),
    }
    session = Session()
    for idx in tqdm(range(0, len(new_contracts), 5)):
        _addresses = new_contracts[idx : idx + 5]
        params["contractaddresses"] = ",".join(_addresses)
        while True:
            res = requests.get(url, params=params)
            if res.status_code == 200:
                break
            time.sleep(1)

        results = res.json()["result"]
        if results is not None:
            for result in results:
                address = result["contractAddress"].lower()
                _addresses.remove(address)

                tx_hash = result["txHash"]
                creation_block = web3.eth.get_transaction(tx_hash)["blockNumber"]

                contract = session.get(Contract, address)
                contract.from_block = creation_block
                session.commit()

        addresses -= set(_addresses)

    session.close()
    return addresses


def dump_files(addresses, path):
    logger.info(f"collecting transfers for {len(addresses)} addresses to {path}")

    latest_block = web3.eth.get_block("latest")["number"]
    logger.info(f"collecting transfers until block {latest_block}")

    # parallel
    pool = Parallel(backend="loky", n_jobs=N_JOBS)
    jobs = [
        delayed(fetch_transfers)(address, latest_block, path) for address in addresses
    ]
    logger.info(f"using {N_JOBS} processes")

    with tqdm_joblib(tqdm(total=len(jobs))) as pbar:
        result = pool(jobs)
    # [fetch_transfers(address, latest_block, path) for address in tqdm(addresses)]


def save_transfers(filepath):
    if not filepath.endswith(".pkl"):
        return

    with gzip.open(filepath, "rb") as f:
        data = pickle.load(f)
    if len(data) == 0:
        return

    session = Session()
    txs = []
    for tx in data:
        token = session.get(Token, tx["address"])
        if token is None:
            continue
        txs.append(
            {
                "block_hash": tx["blockHash"].hex(),
                "tx_hash": tx["transactionHash"].hex(),
                "log_index": tx["logIndex"],
                "block_number": tx["blockNumber"],
                "token_id": tx["address"],
                "from_address": tx["from"],
                "to_address": tx["to"],
                "value": str(tx["value"]),
            }
        )
    if len(txs) == 0:
        return

    stmt = (
        insert(Transfer)
        .values(txs)
        .on_conflict_do_nothing(index_elements=["block_hash", "tx_hash", "log_index"])
    )
    session.execute(stmt)
    session.commit()
    session.close()


def migrate(path):
    logger.info(f"saving transfers in database from {path}")

    # parallel
    pool = Parallel(backend="loky", n_jobs=N_JOBS)
    jobs = [
        delayed(save_transfers)(os.path.join(path, filename))
        for filename in os.listdir(path)
    ]
    logger.info(f"using {N_JOBS} processes")

    with tqdm_joblib(tqdm(total=len(jobs))) as pbar:
        result = pool(jobs)
    # [save_transfers(os.path.join(path, filename)) for filename in os.listdir(path)]


def run(path):
    addresses = create_contracts()
    dump_files(addresses, path)
    # migrate(path)
