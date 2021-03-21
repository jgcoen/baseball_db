import datetime
import logging
import os

import numpy as np
import pandas as pd
from pybaseball.retrosheet import (all_star_game_logs, division_series_logs,
                                   lcs_logs, park_codes, season_game_logs,
                                   wild_card_logs, world_series_logs)
from utils import (configure_logging, load_secrets, pull_single_table,
        sleep_random)
from multi_year_data_pull import MultiYearDataPull


def retrosheet_season_coverage():
    current_year = datetime.datetime.today().year
    potential_coverage = set(np.arange(1871,current_year))
    directory = 'data/retrosheet/season_game_logs_dir/'
    coverage = {int(f.split('.')[0]) for f in os.listdir(directory)}

    uncovered_years = potential_coverage - coverage

    return sorted(uncovered_years,reverse=True)

def pull_season_game_logs(limit: int=10):

    ###Assumptions, doenst update often, so only need to delete and redo the most recent full year
    uncovered_years = retrosheet_season_coverage()
    logging.info(f"There are {len(uncovered_years)} uncovered years to pull")

    for year in uncovered_years[:limit]:
        sleep_random()
        logging.info(f"Pulling data from {year}")
        df = season_game_logs(year)

        path = f'data/retrosheet/season_game_logs_dir/{year}.tsv.gz'
        df.to_csv(path, sep='\t', compression='gzip', index=False)

def concat_tables():
    
    directory = 'data/retrosheet/season_game_logs_dir/'

    files = [f for f in os.listdir(directory) if 'tsv' in f]
    dfs = [pd.read_csv(directory+f,sep='\t', compression='gzip') for f in files]

    df = pd.concat(dfs)
    
    df.to_csv('data/retrosheet/season_game_logs.tsv.gz', sep='\t', index=False, compression='gzip')




def main():
    load_secrets()
    

    # single_table_funcs = [world_series_logs, all_star_game_logs, wild_card_logs,
    #                               division_series_logs, lcs_logs, park_codes]

    # for func in single_table_funcs:
    #     sleep_random()
    #     pull_single_table(func,path_prefix='data/retrosheet/')
    
    # pull_season_game_logs()
    # concat_tables()

    _season_game_logs = MultiYearDataPull(name='season_game_logs', schema ='retrosheet', func=season_game_logs, min_year=1871, limit=70)
    _season_game_logs.update_table()

if __name__ == "__main__":
    configure_logging()
    main()
