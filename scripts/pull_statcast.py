import logging

from data_pull_classes import StatcastDataPull
from pybaseball import cache
from utils import configure_logging


def main():
    cache.enable()

    logging.info('Beginning to update statcast data')
    _statcast = StatcastDataPull(limit=10)
    _statcast.update_table()
    logging.info('Finished updating statcast data')


if __name__ == "__main__":
    configure_logging()
    main()
