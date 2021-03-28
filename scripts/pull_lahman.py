import logging

from pybaseball.lahman import (all_star_full, appearances, awards_managers,
                               awards_players, awards_share_managers,
                               awards_share_players, batting, batting_post,
                               college_playing, fielding, fielding_of,
                               fielding_of_split, fielding_post, hall_of_fame,
                               home_games, managers, managers_half, parks,
                               people, pitching, pitching_post, salaries,
                               schools, series_post, teams, teams_franchises,
                               teams_half)
from utils import configure_logging, pull_single_table, sleep_random

def main():
    
    logging.info('Begining to update lahman data')

    funcs = [people, parks, all_star_full, appearances, awards_managers,
            awards_players, awards_share_managers, awards_share_players,
            batting, batting_post, college_playing, fielding, fielding_of,
            fielding_of_split, fielding_post, hall_of_fame,
            home_games, managers, managers_half, pitching, pitching_post,
            salaries, schools, series_post, teams, teams_franchises,teams_half]

    for func in funcs:
        sleep_random()
        pull_single_table(func, path_prefix='data/lahman/')

    logging.info('Finished updating lahman data')

if __name__ == "__main__":
    configure_logging()
    main()
