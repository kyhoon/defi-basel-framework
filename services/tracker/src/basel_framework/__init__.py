from basel_framework.cet1 import calculate_cet1
from basel_framework.credit import calculate_credit_rwa
from basel_framework.market import calculate_market_rwa
from basel_framework.operational import calculate_operational_rwa


def calculate_rwa():
    pass


def calculate_car():
    calculate_cet1()
    calculate_credit_rwa()
    calculate_market_rwa()
    calculate_operational_rwa()
    calculate_rwa()
