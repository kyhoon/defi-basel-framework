import logging
from decimal import Decimal

import numpy as np
import pandas as pd

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


def get_relevant_protocols(balance):
    protocols = {}
    with Session() as session:
        for token_id in balance.columns:
            if token_id in get_tokens("cash"):
                continue
            protocol = session.get(Token, token_id).protocol
            if protocol in protocols:
                protocols[protocol].append(token_id)
            else:
                protocols[protocol] = [token_id]
    return protocols


def calculate_addons(usd_balance):
    # separate entities
    entities = {}
    with Session() as session:
        for token in usd_balance.columns:
            underlying = session.get(Token, token).underlying
            if underlying in entities:
                entities[underlying].append(token)
            else:
                entities[underlying] = [token]

    # aggregate addons
    addon_sum = pd.Series(Decimal(0.0), index=usd_balance.index)
    addon_sq = pd.Series(Decimal(0.0), index=usd_balance.index)
    for entity, entity_tokens in entities.items():
        # index
        if entity in get_tokens("index"):
            sf = Decimal(0.2)
            rho = Decimal(0.8)
        # single entity
        else:
            sf = Decimal(0.32)
            rho = Decimal(0.5)

        if entity is None or len(entity_tokens) == 1:
            for entity_token in entity_tokens:
                addon_entity = sf * usd_balance[entity_token]
                addon_sum += rho * addon_entity
                addon_sq += (1 - rho**2) * addon_entity**2
        else:
            addon_entity = sf * usd_balance[entity_tokens].sum(axis=1)
            addon_sum += rho * addon_entity
            addon_sq += (1 - rho**2) * addon_entity**2

    addon_agg = (addon_sum**2 + addon_sq).apply(lambda x: Decimal(x).sqrt())
    return addon_agg


def calculate_ccr_rwa(protocol):
    logger.info(f"calculating counterparty credit risk for protocol {protocol.id}")

    balance = get_daily_balance(protocol.id)
    protocols = get_relevant_protocols(balance)

    rwa = pd.Series(Decimal(0.0), index=balance.index)

    for protocol, tokens in protocols.items():
        # exposure at default
        usd_balance = get_usd_balance(balance[tokens])
        addon = calculate_addons(usd_balance)

        V = usd_balance.sum(axis=1)
        multiplier = 0.05 + (1 - 0.05) * np.exp(
            V.astype(float) / (2 * (1 - 0.05) * addon.astype(float))
        )
        multiplier = multiplier.clip(upper=1.0).fillna(0.0).apply(Decimal)
        pfe = multiplier * addon

        ead = Decimal(1.4) * (V + pfe)

        # apply risk weight
        if protocol.rating in ["AAA", "AA"]:
            weight = Decimal(0.2)
        elif protocol.rating in ["A"]:
            weight = Decimal(0.5)
        elif protocol.rating in ["BBB"]:
            weight = Decimal(0.75)
        elif protocol.rating in ["BB"]:
            weight = Decimal(1.0)
        else:
            weight = Decimal(1.5)

        rwa += weight * ead

    return rwa
