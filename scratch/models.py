import os

from dotenv import load_dotenv
from pony import orm

load_dotenv()

db = orm.Database()
db.bind(
    provider="postgres",
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    host=os.getenv("DB_HOST"),
    database=os.getenv("DB_DATABASE"),
)


class Protocol(db.Entity):
    name = orm.PrimaryKey(str)
    treasury = orm.Optional("Contract", reverse="treasury_in")
    contracts = orm.Set("Contract")


class Contract(db.Entity):
    address = orm.PrimaryKey(str)
    protocol = orm.Required(Protocol)
    treasury_in = orm.Optional("Protocol", reverse="treasury")


class Transfer(db.Entity):
    block_number = orm.Required(int)
    block_hash = orm.Required(str)
    tx_hash = orm.Required(str)
    log_index = orm.Required(int)
    token = orm.Required(str)
    from_address = orm.Required(str)
    to_address = orm.Required(str)
    value = orm.Required(str)

    orm.composite_key(block_hash, tx_hash, log_index)


class Token(db.Entity):
    address = orm.PrimaryKey(str)
    category = orm.Required(str)
    decimals = orm.Required(int)
    prices = orm.Set("Price")


class Price(db.Entity):
    token = orm.Required(Token)
    block_number = orm.Required(int)
    price = orm.Required(str)

    orm.composite_key(token, block_number)


db.generate_mapping(create_tables=True)


def commit():
    try:
        db.commit()
    except orm.core.TransactionIntegrityError:
        db.rollback()
