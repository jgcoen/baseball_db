import logging

from data_pull_classes import MultiYearDataPull
from pybaseball import (batting_stats, pitching_stats, team_batting,
                        team_fielding, team_pitching)
from utils import configure_logging


def main():

    logging.info('Begining to pull fangraphs data')

    _batting_stats = MultiYearDataPull(name='batting_stats', schema ='fangraphs', func=batting_stats, min_year=1980, limit=10, kwargs={'qual': 0}, current_year=True)
    _batting_stats.update_table()

    _pitching_stats = MultiYearDataPull(name='pitching_stats', schema ='fangraphs', func=pitching_stats, min_year=1980, limit=10, kwargs={'qual': 0}, current_year=True)
    _pitching_stats.update_table()

    _team_batting = MultiYearDataPull(name='team_batting', schema ='fangraphs', func=team_batting, min_year=1980, limit=10, kwargs={'qual': 0}, current_year=True)
    _team_batting.update_table()

    _team_fielding = MultiYearDataPull(name='team_fielding', schema ='fangraphs', func=team_fielding, min_year=1980, limit=10, kwargs={'qual': 0}, current_year=True)
    _team_fielding.update_table()

    _team_pitching = MultiYearDataPull(name='team_pitching', schema ='fangraphs', func=team_pitching, min_year=1980, limit=10, kwargs={'qual': 0}, current_year=True)
    _team_pitching.update_table()

    logging.info('Finished to pulling fangraphs data')

if __name__ == "__main__":
    configure_logging()
    main()
