import logging
import contextlib
import itertools
import json
import os

from sqlalchemy.exc import IntegrityError
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
from data.models import Protocol, Transfer, Contract

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
N_JOBS = 16

load_dotenv()
web3 = Web3(Web3.HTTPProvider(os.getenv("WEB3_PROVIDER")))

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


def fetch_transfers(address, to_block):
    # get last block
    session = Session()
    contract = session.get(Contract, address)
    if contract is None:
        contract = Contract(id=address, last_block=0)
        session.add(contract)
        session.commit()
        from_block = 0
    else:
        from_block = contract.last_block
    session.close()

    event = ERC20.events.Transfer
    event_abi = event._get_event_abi()
    event_abi_codec = event.web3.codec

    for _from_block in range(from_block, to_block, BLOCK_SIZE):
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
        while True:
            try:
                logs = event.web3.eth.get_logs(event_filter_params)
                break
            except:
                continue
        for entry in logs:
            try:
                _events.append(parse_event(event_abi_codec, event_abi, entry))
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
        while True:
            try:
                logs = event.web3.eth.get_logs(event_filter_params)
                break
            except:
                continue
        for entry in logs:
            try:
                _events.append(parse_event(event_abi_codec, event_abi, entry))
            except:
                continue

        session = Session()
        for _event in _events:
            statement = (
                insert(Transfer)
                .values(
                    block_hash=_event["blockHash"].hex(),
                    tx_hash=_event["transactionHash"].hex(),
                    log_index=_event["logIndex"],
                    block_number=_event["blockNumber"],
                    token_id=_event["address"],
                    from_address=_event["from"],
                    to_address=_event["to"],
                    value=str(_event["value"]),
                )
                .on_conflict_do_nothing(
                    index_elements=["block_hash", "tx_hash", "log_index"]
                )
            )
            try:
                session.execute(statement)
                session.commit()
            except IntegrityError:
                session.rollback()
                continue

        # update last block
        contract = session.get(Contract, address)
        contract.last_block = _to_block
        session.add(contract)
        session.commit()

        session.close()


def run():
    session = Session()
    addresses = set({})
    for protocol in session.query(Protocol).all():
        addresses.update(protocol.treasury)
        addresses.update(protocol.addresses)
    session.close()

    logger.info(f"collecting transfers from {len(addresses)} addresses")

    latest_block = web3.eth.get_block("latest")["number"]
    logger.info(f"collecting transfers until block {latest_block}")

    # parallel
    pool = Parallel(backend="threading", n_jobs=N_JOBS)
    jobs = [delayed(fetch_transfers)(address, latest_block) for address in addresses]
    logger.info(f"using {N_JOBS} processes")

    with tqdm_joblib(tqdm(total=len(jobs))) as pbar:
        result = pool(jobs)
