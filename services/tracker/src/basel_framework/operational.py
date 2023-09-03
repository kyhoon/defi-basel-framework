import logging
from decimal import Decimal

import numpy as np
import pandas as pd

from basel_framework.utils import get_daily_balance, get_tokens, get_usd_prices
from data.base import Session
from data.models import Protocol, Token, Transfer

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
WINDOW = 365


def aggregate_txs(txs, category):
    txs = txs[txs.category == category].value
    txs = txs.groupby(pd.Grouper(freq="D")).sum()
    return txs.apply(Decimal)


def calculate_sc(protocol):
    logger.debug(f"calculating the services component for protocol {protocol.id}")

    with Session() as session:
        protocol = session.get(Protocol, protocol.id)
        treasuries = [treasury.id for treasury in protocol.treasuries]
    fee_income = pd.Series(Decimal(0.0), index=pd.DatetimeIndex([]))
    fee_expense = pd.Series(Decimal(0.0), index=pd.DatetimeIndex([]))
    operating_income = pd.Series(Decimal(0.0), index=pd.DatetimeIndex([]))
    operating_expense = pd.Series(Decimal(0.0), index=pd.DatetimeIndex([]))

    with Session() as session:
        for token_id, decimals in session.query(Token.id, Token.decimals):
            relevant_txs = []
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
                    elif tx.to_address in protocol.addresses:
                        category = "fee_expense"
                    else:
                        category = "operating_expense"
                elif tx.from_address in protocol.addresses:
                    category = "fee_income"
                else:
                    category = "operating_income"

                value = Decimal(tx.value) / Decimal(10**decimals)
                relevant_txs.append(
                    {"timestamp": tx.timestamp, "value": value, "category": category}
                )

            if len(relevant_txs) == 0:
                continue

            relevant_txs = pd.DataFrame(relevant_txs)
            relevant_txs = relevant_txs.set_index(
                pd.to_datetime(relevant_txs["timestamp"], unit="s")
            ).drop(columns=["timestamp"])

            _fee_income = aggregate_txs(relevant_txs, "fee_income")
            _fee_expense = aggregate_txs(relevant_txs, "fee_expense")
            _operating_income = aggregate_txs(relevant_txs, "operating_income")
            _operating_expense = aggregate_txs(relevant_txs, "operating_expense")

            # get price
            prices = Decimal(1.0)
            if token_id not in get_tokens("cash"):
                prices = get_usd_prices(token_id)

            fee_income = fee_income.add(_fee_income * prices, fill_value=Decimal(0.0))
            fee_expense = fee_expense.add(
                _fee_expense * prices, fill_value=Decimal(0.0)
            )
            operating_income = operating_income.add(
                _operating_income * prices, fill_value=Decimal(0.0)
            )
            operating_expense = operating_expense.add(
                _operating_expense * prices, fill_value=Decimal(0.0)
            )

    sc_fee = (
        pd.concat([fee_income, fee_expense], axis=1)
        .resample("D")
        .sum()
        .rolling(WINDOW, min_periods=1)
        .sum()
        .max(axis=1)
    )
    sc_operating = (
        pd.concat([operating_income, operating_expense], axis=1)
        .resample("D")
        .sum()
        .rolling(WINDOW, min_periods=1)
        .sum()
        .max(axis=1)
    )
    return sc_fee.add(sc_operating, fill_value=0.0)


def calculate_fc(protocol):
    logger.debug(f"calculating the financial component for protocol {protocol.id}")

    balance = get_daily_balance(protocol.id)
    pnl = pd.Series(Decimal(0.0), index=balance.index)
    for token_id in balance.columns:
        if token_id in get_tokens("cash"):
            continue
        prices = get_usd_prices(token_id)
        pnl += balance[token_id].shift() * prices.diff()

    return pnl.rolling(WINDOW, min_periods=1).sum().abs()


def calculate_operational_rwa(protocol):
    logger.info(f"calculating operational risk for protocol {protocol.id}")

    # business indicator component
    bi = (
        calculate_sc(protocol)
        .add(calculate_fc(protocol), fill_value=0.0)
        .apply(Decimal)
    )
    thres1 = Decimal(1_000_000_000)
    thres2 = Decimal(30_000_000_000)
    bucket1 = bi.clip(upper=thres1)
    bucket2 = bi.clip(lower=thres1, upper=thres2) - thres1
    bucket3 = bi.clip(lower=thres2) - thres2
    bic = bucket1 * Decimal(0.12) + bucket2 * Decimal(0.15) + bucket3 * Decimal(0.18)

    # internal loss multiplier
    hacks = pd.DataFrame(protocol.hacks)
    if len(hacks) == 0:
        ilm = Decimal(1.0)
    else:
        hacks.date = pd.to_datetime(hacks.date)
        hacks.set_index("date", inplace=True)
        hacks = hacks.reindex(bic.index).fillna(0.0)
        yearly = hacks.rolling(WINDOW).sum()
        lc = 15 * yearly.amount
        ilm = np.log(np.exp(1) - 1 + (lc / bic.astype(float)) ** 0.8).apply(Decimal)
        ilm.replace([np.inf, -np.inf], np.nan, inplace=True)

    orc = bic * ilm
    rwa = Decimal(12.5) * orc

    return rwa
