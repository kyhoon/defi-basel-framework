"""fetch erc20 transfers from/to the addresses"""
import itertools
import json
import os

from dotenv import load_dotenv
from eth_utils import to_bytes
from hexbytes import HexBytes
from joblib import Parallel, delayed
from models import Contract, Transfer, commit
from pony import orm
from tqdm import tqdm
from utils import tqdm_joblib
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

load_dotenv()

BLOCK_SIZE = 10000
web3 = Web3(Web3.HTTPProvider(os.getenv("WEB3_PROVIDER")))

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

    with orm.db_session:
        for event in events:
            Transfer(
                block_number=event["blockNumber"],
                block_hash=event["blockHash"].hex(),
                tx_hash=event["transactionHash"].hex(),
                log_index=event["logIndex"],
                token=event["address"],
                from_address=event["from"],
                to_address=event["to"],
                value=str(event["value"]),
            )
            commit()


if __name__ == "__main__":
    # read addresses
    with orm.db_session:
        targets = list(orm.select(c.address for c in Contract))

    # parallel
    jobs = [delayed(fetch_transfers)(target, 0, "latest") for target in targets]
    pool = Parallel(backend="threading", n_jobs=16)

    with tqdm_joblib(tqdm(total=len(jobs))) as pbar:
        result = pool(jobs)
