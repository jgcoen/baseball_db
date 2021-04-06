import logging
from data_pull_classes import MultiYearDataPull
from pybaseball import standings
from pybaseball import bwar_bat, bwar_pitch, batting_stats_bref, pitching_stats_bref
from utils import configure_logging, pull_single_table
from data_pull_classes import MultiYearDataPull

def main():

    # logging.info('Beginning to pull standings')
    # _standings = MultiYearDataPull(name='standings', schema ='baseball_reference', func=standings, min_year=1969, limit=50, add_year=True, current_year=True)
    # _standings.update_table()

    # logging.info('Beginning to pull Baseball Reference War')

    # pull_single_table(bwar_bat, path_prefix='data/baseball_reference/', kwargs={'return_all': True})

    # pull_single_table(bwar_pitch, path_prefix='data/baseball_reference/', kwargs={'return_all': True})

    _batting_stats = MultiYearDataPull(name='batting_stats_bref', schema ='baseball_reference', func=batting_stats_bref, min_year=2008, limit=15, current_year=True, add_year=True)
    _batting_stats.update_table()

    _pitching_stats = MultiYearDataPull(name='pitching_stats_bref', schema ='baseball_reference', func=pitching_stats_bref, min_year=2008, limit=15, current_year=True, add_year=True)
    _pitching_stats.update_table()


if __name__ == "__main__":
    configure_logging()
    main()