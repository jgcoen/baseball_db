import logging

import pull_lahman
import pull_retrosheet
import pull_statcast
import update_db
from utils import configure_logging


def main():

    logging.info('Beginning to pull data from lahman')
    pull_lahman.main()
    logging.info('Finished pulling data from lahman')

    logging.info('Beginning to pull data from retrosheet')
    pull_retrosheet.main()
    logging.info('Finished pulling data from retrosheet')

    logging.info('Beginning to pull data from statcast')
    pull_statcast.main()
    logging.info('Finished pulling data from retrosheet')

    logging.info('Beginning to update the database')
    update_db.main()
    logging.info('Finished updating the database')

if __name__ == "__main__":
    configure_logging()
    main()
