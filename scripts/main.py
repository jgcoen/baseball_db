import logging

import pull_baseball_reference
import pull_fangraphs
import pull_lahman
import pull_retrosheet
import pull_statcast
import update_db
from data_pull_classes import pull_single_table, MultiYearDataPull
from pybaseball import chadwick_register, amateur_draft
from utils import configure_logging


def main():

    logging.info('Begining to update all the data')

    pull_lahman.main()
    pull_retrosheet.main()
    pull_fangraphs.main()
    pull_statcast.main()
    pull_baseball_reference.main()

    logging.info('Begining to pull chadwick data')
    pull_single_table(func=chadwick_register, path_prefix='data/chadwick/')
    logging.info('Finished pulling chadwick data')

    _draft = MultiYearDataPull(name='amateur_draft', schema ='draft', func=amateur_draft, min_year=1980, limit=10, current_year=True, add_year=True)
    _draft.update_table()

    update_db.main()

    logging.info('Finished updating all the data')

if __name__ == "__main__":
    configure_logging()
    main()