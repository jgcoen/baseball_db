import datetime
import logging

import numpy as np
import pandas as pd
from pybaseball.retrosheet import (all_star_game_logs, division_series_logs,
                                   lcs_logs, park_codes, season_game_logs,
                                   wild_card_logs, world_series_logs)
from utils import (configure_logging, load_secrets, pull_single_table,
                   sleep_random)


def retrosheet_season_coverage():
    current_year = datetime.datetime.today().year
    total_retrosheet_coverage = np.arange(1871,current_year)

    retrosheet_season_data = pd.read_csv('data/retrosheet/season_game_logs.tsv.gz', sep='\t', parse_dates=['date'],compression='gzip')
    retrosheet_season_data['year'] = retrosheet_season_data['date'].dt.year
    yearly_rows = retrosheet_season_data.groupby('year').agg(rows=('game_num','count')).to_dict()['rows']
    print(yearly_rows)
    
    uncovered_years = []
    for year in total_retrosheet_coverage:
        if yearly_rows.get(year,0) > 0:
            pass
        else:
            uncovered_years.append(year)

    return sorted(uncovered_years,reverse=True)

def pull_season_game_logs(limit: int=10):

    ###Assumptions, doenst update often, so only need to delete and redo the most recent full year
    uncovered_years = retrosheet_season_coverage()
    logging.info(f"There are {len(uncovered_years)} uncovered years to pull")

    for year in uncovered_years[:limit]:
        sleep_random()
        logging.info(f"Pulling data from {year}")
        df = season_game_logs(year)

        prev_df = pd.read_csv('data/retrosheet/season_game_logs.tsv.gz', sep='\t',low_memory=False, compression='gzip')
        appended_df = prev_df.append(df).drop_duplicates()
        appended_df.to_csv('data/retrosheet/season_game_logs.tsv.gz', index=False, sep='\t', compression='gzip')




def main():
    load_secrets()
    configure_logging()

    # single_table_funcs = [world_series_logs, all_star_game_logs, wild_card_logs,
    #                               division_series_logs, lcs_logs, park_codes]

    # for func in single_table_funcs:
    #     sleep_random()
    #     pull_single_table(func,path_prefix='data/retrosheet/')
    pull_season_game_logs()


if __name__ == "__main__":
    main()
