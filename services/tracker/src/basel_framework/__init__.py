import logging
from decimal import Decimal

import pandas as pd
from joblib import Parallel, delayed
from sqlalchemy.dialects.postgresql import insert

from basel_framework.cet1 import calculate_cet1
from basel_framework.credit import calculate_ccr_rwa
from basel_framework.market import calculate_market_rwa
from basel_framework.operational import calculate_operational_rwa
from data.base import Session
from data.models import Assets, Protocol

# logger
logger = logging.getLogger(__file__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter(
    "%(asctime)s - %(levelname)s - basel_framework/%(filename)s:%(lineno)s - %(message)s"
)
sh = logging.StreamHandler()
sh.setFormatter(formatter)
logger.addHandler(sh)


def _calculate_car(protocol):
    logger.info(f"calculating CAR for protocol {protocol.id}")

    cet1 = calculate_cet1(protocol)
    ccr_rwa = calculate_ccr_rwa(protocol)
    mar_rwa = calculate_market_rwa(protocol).add(ccr_rwa, fill_value=Decimal(0.0))
    ope_rwa = calculate_operational_rwa(protocol)

    rwa = (
        pd.concat([ccr_rwa, mar_rwa, ope_rwa], axis=1)
        .dropna(how="all")
        .fillna(Decimal(0.0))
    )
    data = pd.concat([cet1, rwa], axis=1).dropna(how="any")

    data.columns = ["cet1", "credit_rwa", "market_rwa", "operational_rwa"]
    data["rwa"] = data.credit_rwa + data.market_rwa + data.operational_rwa
    data["car"] = data.cet1.astype(float) / data.rwa.astype(float)
    data.dropna(inplace=True)

    with Session() as session:
        logger.debug(f"updating {len(data)} CAR values for protocol {protocol.id}")
        for dt, row in data.iterrows():
            stmt = (
                insert(Assets)
                .values(
                    protocol_id=protocol.id,
                    timestamp=dt.value // 10**9,
                    **dict(row),
                )
                .on_conflict_do_update(
                    index_elements=["protocol_id", "timestamp"], set_=dict(row)
                )
            )
            session.execute(stmt)
            session.commit()


def calculate_car():
    with Session() as session:
        protocols = [
            protocol
            for protocol in session.query(Protocol).all()
            if len(protocol.treasuries) > 0
        ]

    Parallel(backend="loky", n_jobs=8)(
        [delayed(_calculate_car)(protocol) for protocol in protocols]
    )
