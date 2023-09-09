from typing import Optional

from fastapi import FastAPI, Query
from models import AssetModel

from data.base import Session
from data.models import Assets

app = FastAPI()


@app.get("/")
def ping() -> str:
    return "FastAPI running on port 8000"


@app.get("/assets/all")
def get_all_assets(
    from_timestamp: Optional[int] = Query(default=0, alias="from"),
    to_timestamp: Optional[int] = Query(default=9999999999, alias="to"),
) -> list[AssetModel]:
    with Session() as session:
        assets = (
            session.query(Assets)
            .filter(
                Assets.timestamp >= from_timestamp,
                Assets.timestamp < to_timestamp,
            )
            .all()
        )
        assets = [
            AssetModel(
                protocol=asset.protocol_id,
                timestamp=asset.timestamp,
                cet1=asset.cet1,
                credit_rwa=asset.credit_rwa,
                market_rwa=asset.market_rwa,
                operational_rwa=asset.operational_rwa,
                rwa=asset.rwa,
            )
            for asset in assets
        ]
    return assets


@app.get("/assets/{protocol_id}")
def get_single_assets(
    protocol_id: str,
    from_timestamp: Optional[int] = Query(default=0, alias="from"),
    to_timestamp: Optional[int] = Query(default=9999999999, alias="to"),
) -> list[AssetModel]:
    with Session() as session:
        assets = (
            session.query(Assets)
            .filter(
                Assets.protocol_id == protocol_id,
                Assets.timestamp >= from_timestamp,
                Assets.timestamp < to_timestamp,
            )
            .all()
        )
        assets = [
            AssetModel(
                protocol=asset.protocol_id,
                timestamp=asset.timestamp,
                cet1=asset.cet1,
                credit_rwa=asset.credit_rwa,
                market_rwa=asset.market_rwa,
                operational_rwa=asset.operational_rwa,
                rwa=asset.rwa,
            )
            for asset in assets
        ]
    return assets
