import logging
from utils import configure_logging
from pybaseball import batting_stats, pitching_stats, team_batting, \
                       team_fielding, team_pitching
from data_pull_classes import MultiYearDataPull

def main():
    
    _batting_stats = MultiYearDataPull(name='batting_stats', schema ='fangraphs', func=batting_stats, min_year=1980, limit=10)
    _batting_stats.update_table()

    _pitching_stats = MultiYearDataPull(name='pitching_stats', schema ='fangraphs', func=pitching_stats, min_year=1980, limit=10)
    _pitching_stats.update_table()

    _team_batting = MultiYearDataPull(name='team_batting', schema ='fangraphs', func=team_batting, min_year=1980, limit=10)
    _team_batting.update_table()

    _team_fielding = MultiYearDataPull(name='team_fielding', schema ='fangraphs', func=team_fielding, min_year=1980, limit=10)
    _team_fielding.update_table()

    _team_pitching = MultiYearDataPull(name='_team_pitching', schema ='fangraphs', func=team_pitching, min_year=1980, limit=10)
    _team_pitching.update_table()

if __name__ == "__main__":
    configure_logging()
    main()
