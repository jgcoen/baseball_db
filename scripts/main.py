import logging

import pull_lahman
import pull_retrosheet
import pull_statcast
import update_db
from utils import configure_logging, pull_single_table
from pybaseball import chadwick_register


def main():

    logging.info('Begining to update all the data')

    #pull_lahman.main()
    pull_retrosheet.main()
    pull_single_table(func=chadwick_register, path_prefix='data/chadwick/')
    pull_statcast.main()
    update_db.main()

    logging.info('Finished update all the data')

if __name__ == "__main__":
    configure_logging()
    main()
