from sqlalchemy import Column, String, Integer, ForeignKey, ARRAY
from sqlalchemy.orm import relationship
from data.base import Base


class Protocol(Base):
    __tablename__ = "protocols"

    id = Column(String, primary_key=True)
    rating = Column(String)
    treasury = Column(ARRAY(String))
    addresses = Column(ARRAY(String))


class Token(Base):
    __tablename__ = "tokens"

    id = Column(String, primary_key=True)
    protocol_id = Column(String, ForeignKey("protocols.id"))
    protocol = relationship("Protocol")
    symbol = Column(String)
    itin = Column(String)
    decimals = Column(Integer)
    prices = relationship("Price")


class Price(Base):
    __tablename__ = "prices"

    block_number = Column(Integer, primary_key=True)
    token_id = Column(String, ForeignKey("tokens.id"), primary_key=True)
    usd_value = Column(String)


class Transfer(Base):
    __tablename__ = "transfers"

    id = Column(Integer, primary_key=True)
    block_hash = Column(String)
    tx_hash = Column(String)
    log_index = Column(Integer)
    block_number = Column(Integer)
    token = Column(String)
    from_address = Column(String)
    to_address = Column(String)
    value = Column(String)


class Contract(Base):
    __tablename__ = "contracts"

    id = Column(String, primary_key=True)
    from_block = Column(Integer, nullable=True)
