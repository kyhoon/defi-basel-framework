"""fetch erc20 transfers from/to the addresses"""
import contextlib
import itertools
import json
import os

import joblib
import pandas as pd
from eth_utils import to_bytes
from hexbytes import HexBytes
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

DATA_DIR = "../data"
BLOCK_SIZE = 100000
web3 = Web3(Web3.HTTPProvider("http://localhost:8547"))

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
        "event": event_abi["name"],
        "logIndex": log_entry["logIndex"],
        "transactionIndex": log_entry["transactionIndex"],
        "transactionHash": log_entry["transactionHash"],
        "address": log_entry["address"],
        "blockHash": log_entry["blockHash"],
        "blockNumber": log_entry["blockNumber"],
        **event_args,
    }
    return event_data


def fetch_transfers(address, from_block=0, to_block="latest"):
    events = []
    event = ERC20.events.Transfer
    event_abi = event._get_event_abi()
    event_abi_codec = event.web3.codec

    if from_block == "latest":
        from_block = web3.eth.get_block("latest")["number"]
    if to_block == "latest":
        to_block = web3.eth.get_block("latest")["number"]

    for _from_block in range(from_block, to_block, BLOCK_SIZE):
        _to_block = min(to_block, _from_block + BLOCK_SIZE)

        # from address
        _, event_filter_params = construct_event_filter_params(
            event_abi,
            event_abi_codec,
            argument_filters={"from": address.lower()},
            fromBlock=_from_block,
            toBlock=_to_block,
        )
        logs = event.web3.eth.get_logs(event_filter_params)
        for entry in logs:
            try:
                events.append(parse_event(event_abi_codec, event_abi, entry))
            except:
                continue

        # to address
        _, event_filter_params = construct_event_filter_params(
            event_abi,
            event_abi_codec,
            argument_filters={"to": address.lower()},
            fromBlock=_from_block,
            toBlock=_to_block,
        )
        logs = event.web3.eth.get_logs(event_filter_params)
        for entry in logs:
            try:
                events.append(parse_event(event_abi_codec, event_abi, entry))
            except:
                continue

    transfers = pd.DataFrame(events)
    transfers.to_csv(os.path.join(DATA_DIR, "transfers", address + ".csv"))


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


if __name__ == "__main__":
    # read addresses
    with open(os.path.join(DATA_DIR, "addresses.json"), "r") as f:
        addresses = json.load(f)
    targets = [addresses["treasury"]] + addresses["addresses"]

    # parallel
    jobs = [delayed(fetch_transfers)(target, 0, 16751554) for target in targets]
    pool = Parallel(backend="threading", n_jobs=16)

    with tqdm_joblib(tqdm(total=len(jobs))) as pbar:
        result = pool(jobs)
