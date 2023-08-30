import logging
import pandas as pd

from data.base import Session
from data.models import Token, Protocol, Transfer

# logger
logger = logging.getLogger(__file__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter(
    "%(asctime)s - %(levelname)s - %(filename)s:%(lineno)s - %(message)s"
)
sh = logging.StreamHandler()
sh.setFormatter(formatter)
logger.addHandler(sh)

# config
OFFSET = 100000


token_map = {
    "cash": [
        "EEP21PP01USD",
    ],
    "equity": [
        "EEP22G",
        "EEP22NT02",
        "EEP22TU03",
        "EEP23E",
        "EEP23EQ",
    ],
    "index": ["EEP23FD"],
    "commodity": [
        "EEP23A",
        "EEP23ER",
    ],
    "fx": [
        "EEP21PP01CHF",
        "EEP21PP01EUR",
    ],
    "settlement": [
        "EEP22S",
        "EEP22TU01",
        "EEP22TU02",
    ],
    "derivative": [
        "EEP23DV",
        "EEP23DV03",
    ],
}


def get_tokens(category):
    assert (
        category in token_map
    ), f"token category should be one of the following: {list(token_map.keys())}"

    token_list = token_map[category]
    with Session() as session:
        tokens = [
            token.id
            for token in session.query(Token.id).filter(Token.itc_eep.in_(token_list))
        ]
    return tokens


def get_daily_balance(protocol_id):
    logger.debug(f"fetching daily balance for protocol {protocol_id}")

    with Session() as session:
        protocol = session.get(Protocol, protocol_id)
        assert protocol is not None, f"unknown protocol id {protocol_id}"

    with Session() as session:
        for token in session.query(Token.id):
            for tx in (
                session.query(Transfer)
                .filter(Transfer.token_id == token.id)
                .filter(
                    Transfer.from_address.in_(protocol.treasury)
                    | Transfer.to_address.in_(protocol.treasury)
                )
            ):
                breakpoint()

    breakpoint()
