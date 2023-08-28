import logging

# logger
logger = logging.getLogger(__file__)
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter(
    "%(asctime)s - %(levelname)s - %(filename)s:%(lineno)s - %(message)s"
)
sh = logging.StreamHandler()
sh.setFormatter(formatter)
logger.addHandler(sh)


def main():
    pass


if __name__ == "__main__":
    main()
