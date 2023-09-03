import logging
from decimal import Decimal

from basel_framework.utils import get_daily_balance, get_tokens, get_usd_balance
from data.base import Session
from data.models import Token

# logger
logger = logging.getLogger(__file__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter(
    "%(asctime)s - %(levelname)s - basel_framework/%(filename)s:%(lineno)s - %(message)s"
)
sh = logging.StreamHandler()
sh.setFormatter(formatter)
logger.addHandler(sh)


def calculate_cet1(protocol):
    logger.debug(f"calculating CET1 for protocol {protocol.id}")

    balance = get_daily_balance(protocol.id)

    cash_tokens = balance.columns[balance.columns.isin(get_tokens("cash"))]
    cash_balance = balance[cash_tokens].sum(axis=1)

    share_tokens = balance.columns[balance.columns.isin(get_tokens("equity"))]
    with Session() as session:
        share_tokens = [
            token
            for token in share_tokens
            if session.get(Token, token).protocol.id == protocol.id
        ]
    share_balance = get_usd_balance(balance[share_tokens]).sum(axis=1)

    cet1 = cash_balance.apply(Decimal) + share_balance.apply(Decimal)
    return cet1
