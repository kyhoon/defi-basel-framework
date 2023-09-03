from typing import Optional

from sqlalchemy import ARRAY, ForeignKey, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from data.base import Base


class Protocol(Base):
    __tablename__ = "protocols"

    id: Mapped[str] = mapped_column(primary_key=True)
    rating: Mapped[str] = mapped_column(String(3))
    treasuries: Mapped[list["Treasury"]] = relationship(back_populates="protocol")
    addresses: Mapped[list[str]] = mapped_column(ARRAY(String(42)))
    hacks: Mapped[list[JSONB]] = mapped_column(ARRAY(JSONB))


class Treasury(Base):
    __tablename__ = "treasuries"

    id: Mapped[str] = mapped_column(String(42), primary_key=True)
    protocol_id: Mapped[str] = mapped_column(ForeignKey("protocols.id"))
    protocol: Mapped["Protocol"] = relationship()


class Token(Base):
    __tablename__ = "tokens"

    id: Mapped[str] = mapped_column(String(42), primary_key=True)
    protocol_id: Mapped[str] = mapped_column(ForeignKey("protocols.id"))
    protocol: Mapped["Protocol"] = relationship()
    symbol: Mapped[str]
    itin: Mapped[str]
    itc_eep: Mapped[Optional[str]]
    underlying: Mapped[Optional[str]] = mapped_column(String(42))
    decimals: Mapped[int]
    prices: Mapped[list["Price"]] = relationship(back_populates="token")


class TransferSnapshot(Base):
    __tablename__ = "transfer_snapshots"

    treasury_id: Mapped[str] = mapped_column(
        ForeignKey("treasuries.id"), primary_key=True
    )
    treasury: Mapped["Treasury"] = relationship()
    from_timestamp: Mapped[int] = mapped_column(primary_key=True)
    to_timestamp: Mapped[int] = mapped_column(primary_key=True)

    def __str__(self):
        return f"{self.treasury_id}-{self.from_timestamp}-{self.to_timestamp}"


class PriceSnapshot(Base):
    __tablename__ = "price_snapshots"

    token_id: Mapped[str] = mapped_column(ForeignKey("tokens.id"), primary_key=True)
    token: Mapped["Token"] = relationship()
    timestamp: Mapped[int] = mapped_column(primary_key=True)

    def __str__(self):
        return f"{self.token_id}-{self.timestamp}"


class Transfer(Base):
    __tablename__ = "transfers"

    id: Mapped[str] = mapped_column(primary_key=True)
    timestamp: Mapped[int]
    block_number: Mapped[int]
    token_id: Mapped[str] = mapped_column(ForeignKey("tokens.id"))
    token: Mapped["Token"] = relationship()
    from_address: Mapped[str] = mapped_column(String(42))
    to_address: Mapped[str] = mapped_column(String(42))
    value: Mapped[str]


class Price(Base):
    __tablename__ = "prices"

    token_id: Mapped[str] = mapped_column(ForeignKey("tokens.id"), primary_key=True)
    token: Mapped["Token"] = relationship()
    timestamp: Mapped[int] = mapped_column(primary_key=True)
    value: Mapped[str]


class Assets(Base):
    __tablename__ = "assets"

    protocol_id: Mapped[str] = mapped_column(
        ForeignKey("protocols.id"), primary_key=True
    )
    protocol: Mapped["Protocol"] = relationship()
    timestamp: Mapped[int] = mapped_column(primary_key=True)
    cet1: Mapped[str]
    credit_rwa: Mapped[str]
    market_rwa: Mapped[str]
    operational_rwa: Mapped[str]
    rwa: Mapped[str]
    car: Mapped[float]
