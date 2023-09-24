import logging
import os

from apscheduler.schedulers.blocking import BlockingScheduler
from basel_framework import calculate_car
from prices import collect_prices
from snapshots import initialize_snapshots, update_snapshots
from transfers import collect_transfers

from data.base import Base, Session, engine
from data.models import Assets, PriceSnapshot, Protocol, Token, TransferSnapshot

# logger
logger = logging.getLogger(__file__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter(
    "%(asctime)s - %(levelname)s - %(filename)s:%(lineno)s - %(message)s"
)
sh = logging.StreamHandler()
sh.setFormatter(formatter)
logger.addHandler(sh)

logging.getLogger("apscheduler").setLevel(logging.CRITICAL)


def heartbeat():
    with Session() as session:
        protocols = len(
            [
                protocol
                for protocol in session.query(Protocol).all()
                if len(protocol.treasuries) > 0
            ]
        )
        tokens = session.query(Token).count()
        assets = session.query(Assets).count()
        transfer_snapshots = session.query(TransferSnapshot).count()
        price_snapshots = session.query(PriceSnapshot).count()

    logger.debug(f"data collected - protocol {protocols}, token {tokens}, CAR {assets}")
    logger.debug(
        f"snapshots left - transfer {transfer_snapshots}, price {price_snapshots}"
    )


def initialize():
    logger.info("initializing database")
    Base.metadata.create_all(engine)
    initialize_snapshots()


def main():
    logger.info("initializing main loop")
    scheduler = BlockingScheduler(job_defaults={"timezone": "UTC"})
    scheduler.add_job(heartbeat, "interval", minutes=1)
    scheduler.add_job(collect_prices, "interval", seconds=1)
    scheduler.add_job(collect_transfers, "interval", seconds=1, max_instances=8)
    scheduler.add_job(update_snapshots, "cron", hour=0)
    scheduler.add_job(calculate_car, "cron", hour=1)

    logger.info(
        "running main loop, press Ctrl+{} to exit".format(
            "Break" if os.name == "nt" else "C"
        )
    )
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("terminating main loop")
        scheduler.shutdown()


if __name__ == "__main__":
    initialize()
    main()
