from pydantic import BaseModel


class AssetModel(BaseModel):
    protocol: str
    timestamp: int
    cet1: str
    credit_rwa: str
    market_rwa: str
    operational_rwa: str
    rwa: str
