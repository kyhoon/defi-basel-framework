import logging
from data.base import Session
from data.models import Protocol, Token, Transfer, Assets
from basel_framework.utils import get_tokens, get_daily_balance

# logger
logger = logging.getLogger(__file__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter(
    "%(asctime)s - %(levelname)s - %(filename)s:%(lineno)s - %(message)s"
)
sh = logging.StreamHandler()
sh.setFormatter(formatter)
logger.addHandler(sh)


def calculate_cet1():
    get_daily_balance("olympus-dao")

    breakpoint()
    pass
