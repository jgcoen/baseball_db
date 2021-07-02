import logging

from data_pull_classes import MultiYearDataPull, pull_single_table
from pybaseball import (batting_stats_bref, bwar_bat, bwar_pitch,
                        pitching_stats_bref, standings, schedule_and_record)
from utils import configure_logging


def main():

    logging.info('Beginning to pull baseball reference data')
    _standings = MultiYearDataPull(name='standings', schema ='baseball_reference', func=standings, min_year=1969, limit=50, add_year=True, current_year=True)
    _standings.update_table()

    logging.info('Beginning to pull Baseball Reference War')

    pull_single_table(bwar_bat, path_prefix='data/baseball_reference/', kwargs={'return_all': True})

    pull_single_table(bwar_pitch, path_prefix='data/baseball_reference/', kwargs={'return_all': True})

    _batting_stats = MultiYearDataPull(name='batting_stats_bref', schema ='baseball_reference', func=batting_stats_bref, min_year=2008, limit=15, current_year=True, add_year=True)
    _batting_stats.update_table()

    _pitching_stats = MultiYearDataPull(name='pitching_stats_bref', schema ='baseball_reference', func=pitching_stats_bref, min_year=2008, limit=15, current_year=True, add_year=True)
    _pitching_stats.update_table()

    _schedule_and_record = MultiYearDataPull(name='schedule_and_record', schema ='baseball_reference', func=schedule_and_record, min_year=1871, limit=15, current_year=True, add_year=True)
    _schedule_and_record.update_table()

if __name__ == "__main__":
    configure_logging()
    main()
