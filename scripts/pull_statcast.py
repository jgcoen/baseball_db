import logging

import pandas as pd
from multi_year_data_pull import StatcastDataPull
from pybaseball import cache, statcast
from utils import configure_logging


def main():
    cache.enable()

    logging.info('Beginning to update statcast data')
    _statcast = StatcastDataPull(limit=12)
    _statcast.update_table()


if __name__ == "__main__":
    configure_logging()
    main()
