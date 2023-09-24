import logging
from decimal import Decimal

import numpy as np
import pandas as pd
from basel_framework.utils import (
    get_daily_balance,
    get_token_category,
    get_tokens,
    get_usd_balance,
    get_usd_prices,
)

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

# config
WINDOW = 365


def calculate_sensitivities(token_id, underlying, token_balance):
    V = get_usd_prices(token_id).astype(float)
    S = get_usd_prices(underlying).astype(float)
    quantity = token_balance.astype(float) / V

    delta = (V.diff() / S.diff()).rolling(WINDOW, min_periods=1).median()
    delta.name = token_id

    sigma = np.sqrt(
        np.log(S).diff().pow(2).rolling(3, min_periods=1, center=True).sum()
    )
    vega = (V.diff() / sigma.diff()).rolling(WINDOW, min_periods=1).median()
    vega.name = token_id

    return delta * quantity, vega * sigma * quantity


def aggregate_buckets(delta_buckets, vega_buckets, weight, rho, gamma):
    # within bucket aggregation
    delta_K, delta_S = [], []
    vega_K, vega_S = [], []
    for bucket in delta_buckets:
        deltas = weight * pd.concat(delta_buckets[bucket], axis=1)
        vegas = weight * pd.concat(vega_buckets[bucket], axis=1)

        _delta_K = deltas.pow(2).sum(axis=1)
        _vega_K = vegas.pow(2).sum(axis=1)
        for k in deltas.columns:
            for l in deltas.columns:
                if k != l:
                    _delta_K += rho * deltas[k] * deltas[l]
                    _vega_K += rho * vegas[k] * vegas[l]

        delta_K.append(np.sqrt(_delta_K))
        vega_K.append(np.sqrt(_vega_K))

        delta_S.append(deltas.sum(axis=1))
        vega_S.append(vegas.sum(axis=1))

    # across bucket aggregation
    delta_K = pd.concat(delta_K, axis=1)
    delta_S = pd.concat(delta_S, axis=1)
    vega_K = pd.concat(vega_K, axis=1)
    vega_S = pd.concat(vega_S, axis=1)

    delta_net = delta_K.pow(2).sum(axis=1)
    vega_net = vega_K.pow(2).sum(axis=1)
    for k in delta_S.columns:
        for l in delta_S.columns:
            if k != l:
                delta_net += gamma * delta_S[k] * delta_S[l]
                vega_net += gamma * vega_S[k] * vega_S[l]

    return np.sqrt(delta_net) + np.sqrt(vega_net)


def calculate_market_rwa(protocol):
    logger.info(f"calculating market risk for protocol {protocol.id}")

    balance = get_daily_balance(protocol.id)

    # sensitivities
    logger.debug(f"calculating sensitivities for protocol {protocol.id}")
    delta_buckets = {}
    vega_buckets = {}
    with Session() as session:
        for token_id in balance.columns:
            if token_id in get_tokens("cash"):
                continue
            underlying = session.get(Token, token_id).underlying
            if underlying is None:
                continue

            delta, vega = calculate_sensitivities(
                token_id, underlying, balance[token_id]
            )
            category = get_token_category(underlying)
            if category in delta_buckets:
                delta_buckets[category].append(delta)
                vega_buckets[category].append(vega)
            else:
                delta_buckets[category] = [delta]
                vega_buckets[category] = [vega]

    if len(delta_buckets) > 0:
        sensitivities = (
            pd.concat(
                [
                    aggregate_buckets(delta_buckets, vega_buckets, 0.7, 0.075, 0.15),
                    aggregate_buckets(
                        delta_buckets, vega_buckets, 0.7, 0.09375, 0.1875
                    ),
                    aggregate_buckets(
                        delta_buckets, vega_buckets, 0.7, 0.05625, 0.1125
                    ),
                ],
                axis=1,
            )
            .max(axis=1)
            .apply(Decimal)
        )
    else:
        sensitivities = Decimal(0.0)

    # default risk capital requirements
    logger.debug(f"calculating default and residual risk for protocol {protocol.id}")
    drc_rrao = get_usd_balance(balance)
    with Session() as session:
        for token_id in drc_rrao.columns:
            if token_id in get_tokens("cash"):
                drc_rrao[token_id] = Decimal(0.0)
                continue

            rating = session.get(Token, token_id).protocol.rating
            if rating == "AAA":
                weight = Decimal(0.005)
            elif rating == "AA":
                weight = Decimal(0.02)
            elif rating == "A":
                weight = Decimal(0.03)
            elif rating == "BBB":
                weight = Decimal(0.06)
            elif rating == "BB":
                weight = Decimal(0.15)
            elif rating == "B":
                weight = Decimal(0.30)
            else:
                weight = Decimal(0.50)
            drc_rrao[token_id] *= weight + Decimal(0.001)  # RRAO

    drc_rrao = drc_rrao.sum(axis=1)

    rwa = Decimal(12.5) * (sensitivities + drc_rrao)

    return rwa
