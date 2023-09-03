import logging
from datetime import datetime, timedelta
from decimal import Decimal

import pandas as pd

from data.base import Session
from data.models import Price, Protocol, Token, Transfer

# logger
logger = logging.getLogger(__file__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter(
    "%(asctime)s - %(levelname)s - basel_framework/%(filename)s:%(lineno)s - %(message)s"
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


def get_token_category(token_id):
    with Session() as session:
        itc_eep = session.get(Token, token_id).itc_eep
    for key, values in token_map.items():
        if itc_eep in values:
            return key
    raise KeyError(f"category unknown for {token_id}")


def get_daily_balance(protocol_id):
    logger.debug(f"fetching daily balance for protocol {protocol_id}")

    with Session() as session:
        protocol = session.get(Protocol, protocol_id)
        assert protocol is not None, f"unknown protocol id {protocol_id}"
        treasuries = [treasury.id for treasury in protocol.treasuries]

    balance = []
    with Session() as session:
        for token_id, decimals in session.query(Token.id, Token.decimals):
            token_txs = []
            for tx in (
                session.query(Transfer)
                .filter(Transfer.token_id == token_id)
                .filter(
                    Transfer.from_address.in_(treasuries)
                    | Transfer.to_address.in_(treasuries)
                )
            ):
                if tx.from_address in treasuries:
                    if tx.to_address in treasuries:
                        continue
                    else:
                        value = -Decimal(tx.value)
                else:
                    value = Decimal(tx.value)
                value /= Decimal(10**decimals)

                token_txs.append({"timestamp": tx.timestamp, "value": value})

            if len(token_txs) == 0:
                continue

            token_df = pd.DataFrame(token_txs)
            token_dt = pd.to_datetime(token_df["timestamp"], unit="s")
            token_df = token_df.set_index(token_dt).drop(columns=["timestamp"]).value
            token_df = token_df.groupby(pd.Grouper(freq="D")).sum()

            token_balance = token_df.cumsum()
            token_balance.name = token_id

            while (token_balance < 0).any():
                logger.warning(
                    f"negative daily balance of token {token_id} for protocol {protocol_id}"
                )
                idx = token_balance[token_balance < 0].index[0]
                diff = -token_balance[idx]
                token_balance[idx:] += diff

            balance.append(token_balance)

    if len(balance) == 0:
        return pd.DataFrame(None)

    balance = pd.concat(balance, axis=1).fillna(method="ffill").fillna(Decimal(0.0))
    index = pd.date_range(
        start=balance.index[0], end=datetime.now() - timedelta(days=1), freq="D"
    )
    balance = balance.reindex(index, method="ffill")
    return balance


def get_usd_prices(token_id):
    with Session() as session:
        prices = (
            session.query(Price.timestamp, Price.value)
            .filter(Price.token_id == token_id)
            .all()
        )
        prices_df = pd.DataFrame(prices)
        if len(prices_df) == 0:
            logger.warning(f"could not find price data for token {token_id}")
            return prices_df

        prices_dt = pd.to_datetime(prices_df["timestamp"], unit="s")
        prices_df = prices_df.set_index(prices_dt).drop(columns=["timestamp"]).value
        prices_df = (
            prices_df.groupby(pd.Grouper(freq="D")).last().fillna(method="ffill")
        )
    return prices_df.apply(Decimal)


def get_usd_balance(balance):
    balance = balance.copy()
    for token_id in balance.columns:
        if token_id in get_tokens("cash"):
            continue
        prices_df = get_usd_prices(token_id)
        token_index = balance[token_id].dropna().index
        prices_df = prices_df.reindex(token_index).fillna(method="ffill")
        if prices_df.isna().any():
            idx = prices_df.index[prices_df.isna()][-1]
            logger.warning(f"missing prices of token {token_id} before {idx}")
            prices_df.fillna(Decimal(0.0), inplace=True)

        balance.loc[:, token_id] *= prices_df

    return balance
