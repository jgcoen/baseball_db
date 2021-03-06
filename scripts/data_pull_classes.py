import datetime
import logging
import os
from typing import Callable

import numpy as np
import pandas as pd
from pybaseball import amateur_draft, statcast, schedule_and_record
from pybaseball.statcast_fielding import statcast_outs_above_average
from utils import configure_logging, sleep_random


class MultiYearDataPull:

    def __init__(self, name, schema, func, min_year, limit: int=10, add_year: bool=False, current_year: bool=False, kwargs: dict={}):
        self.name = name
        self.schema = schema
        self.func = func
        self.min_year = min_year
        self.limit = limit
        self.add_year = add_year
        self.current_year = current_year
        self.kwargs = kwargs

        #Paths
        self.table_path = f'data/{schema}/{name}.tsv.gz'
        self.directory_path = f'data/{schema}/{name}_dir/'

        #Coverage
        self.potential_coverage = None
        self.coverage = None
    
    def _create_directory(self):
        
        if not os.path.isdir(self.directory_path):
            os.mkdir(self.directory_path)

    def _find_potential_coverage(self) -> set:
        """ Finds the potential years of data to pull, using the min_year

        Returns:
            potential_coverage (set): set of years that could potentially be pulled
        """

        current_year = datetime.datetime.today().year

        if self.current_year:
            current_year+=1

        potential_coverage = set(np.arange(self.min_year,current_year))

        return potential_coverage

    def _find_coverage(self) -> set:
        """ Finds the years of data that have been pulled

        Returns:
            coverage (set): set of years that have already been pulled
        """

        #Assumes files are in yyyy.tsv.gz format
        coverage = {int(f.split('.')[0]) for f in os.listdir(self.directory_path) if 'tsv' in f}

        return coverage

    def _pull_data(self) -> None:
        """Pulls the data based on years that are currently uncovered using self.func.
           Writes a gzip tsv to self.directory_path/year.tsv.gz
        """

        outstanding_years = sorted(self.potential_coverage - self.coverage, reverse=True)
        logging.info(f"There are {len(outstanding_years)} years to pull, pulling {min([self.limit,len(outstanding_years)])} now")

        for year in outstanding_years[:self.limit]:
            sleep_random(min_seconds=5, max_seconds=10)
            year_path = f"{self.directory_path}{year}.tsv.gz"

            try:
                #This is amateur draft specifc
                if self.func==amateur_draft:
                    _round = 1
                    df = []
                    while _round <=100: #Max 100 rounds
                        sleep_random(min_seconds=1, max_seconds=2)
                        try:
                            round_df = self.func(year,_round, **self.kwargs)
                            logging.info(f"Pulled data from the draft, year={year}, round={_round}")
                            round_df['round'] = _round
                            df.append(round_df)
                            _round += 1
                        except ImportError: #Now the draft is over, can move on
                            break
                #statcast_outs_above_average specific
                elif self.func==statcast_outs_above_average:
                    df = []
                    for pos in range(3,10):
                        pos_df = statcast_outs_above_average(year, pos, **self.kwargs)
                        df.append(pos_df)
                
                elif self.func == schedule_and_record:
                    bwar_bat_path = 'data/baseball_reference/bwar_bat.tsv.gz'
                    bwar_bat = pd.read_csv('data/baseball_reference/bwar_bat.tsv.gz', sep='\t')
                    teams = bwar_bat.loc[bwar_bat['year_ID']==year, 'team_ID'].unique()
                    df = []
                    for t in teams:
                        sleep_random(min_seconds=.5, max_seconds=1)
                        team_df = schedule_and_record(season=year, team=t)
                        team_df['orig_scheduled'] = team_df['Orig. Scheduled']
                        team_df = team_df.drop(columns='Orig. Scheduled')
                        df.append(team_df)
                else:
                    df = self.func(year, **self.kwargs)

                #For standings and amateur draft, returns list of dfs
                if isinstance(df,list):
                    df = pd.concat(df)
                if self.add_year:
                    df['year'] = year

                #Remove The weird date empty string from 2016
                if self.schema=='retrosheet' and self.name=='schedules':
                    df = df.loc[df['date'].str.len() >1]

                df.to_csv(year_path, index=False, sep='\t', compression='gzip')

                logging.info(f"Wrote data to {year_path}")
            except:
                logging.info(f"Could not pull data for {self.name} from {year}")
                if year==2019:
                    df = self.func(year, **self.kwargs)


    def _aggregate_data(self) -> None:
        """ (DEPRECATED) Aggregates all data in the self.directory_path and writes it to self.path"""

        logging.info(f"Begining to aggreate data and refresh {self.table_path}")
        files = [f for f in os.listdir(self.directory_path) if 'tsv' in f]
        dfs = [pd.read_csv(self.directory_path+f,sep='\t', compression='gzip') for f in files if 'tsv' in f]
        df = pd.concat(dfs)
        df.to_csv(self.table_path, sep='\t', index=False, compression='gzip')
        logging.info(f"Finished aggregating data and refreshing {self.table_path}")

    def _remove_most_recent_data(self) -> None:
        """Removes the most recent data in self.directory_path to refresh"""

        #Test to see if there there any files that are not in yyyy format
        bad_format = [f for f in os.listdir(self.directory_path) if 'tsv' and len(f.split('.')[0])>4]
        
        if len(bad_format)>0:
            for bad_file in bad_format:
                bad_file_path = f"{self.directory_path}{bad_file}"
                logging.info(f"Removing malformed files: {bad_file_path}")
                os.remove(bad_file_path)


        coverage = {int(f.split('.')[0]) for f in os.listdir(self.directory_path) if 'tsv' in f}
        if coverage != set():
            max_year = max(coverage)
            most_recent_path = f"{self.directory_path}{max_year}.tsv.gz"
            os.remove(most_recent_path)

            logging.info(f"Just removed {most_recent_path} to refresh data for {self.name}")
        else:
            logging.info('There is no data to remove')


    def update_table(self):
        """Updates the table

           Steps
            - check if there is a directory
            - remove the most recent data
            - determine current coverage
            - pull data
            - aggregate the data in the directory to the self.path
        """

        logging.info(f"Begining to pull update the data for {self.name}")

        self._create_directory()

        #Remove Most Recent Data
        self._remove_most_recent_data()

        #Coverage
        self.potential_coverage = self._find_potential_coverage()
        self.coverage = self._find_coverage()

        self._pull_data()

        logging.info(f"Finished updating the data for {self.name}")

class StatcastDataPull(MultiYearDataPull):

    def __init__(self, name: str='statcast', schema: str='statcast', func=statcast, min_year: int=2008, limit: int=4):
        self.name = name
        self.schema = schema
        self.func = func
        self.min_year = min_year
        self.limit = limit

        #Paths
        self.table_path = f'data/{schema}/{name}.tsv.gz'
        self.directory_path = f'data/{schema}/{name}_dir/'

        #Coverage
        self.potential_coverage = None
        self.coverage = None
        self.month_start_end = None

    def _find_potential_coverage(self) -> set:
        """ Finds the potential monhts of data to pull, using the min_year

        Statcast data is pulled on a monthly interval, valid months are March - November

        Returns:
            potential_coverage (set): set of years that could potentially be pulled
        """

        current_date = datetime.datetime.today()+datetime.timedelta(days=28)
        min_month = f"{self.min_year}-03-01" #set it to march of the min year
        month_start = [d.strftime('%Y-%m-%d') for d in pd.date_range(start=min_month, end=current_date, freq='MS') if d.month not in [1,12]]
        month_end = [d.strftime('%Y-%m-%d') for d in pd.date_range(start=min_month, end=current_date, freq='M') if d.month not in [1,12]]
        potential_coverage = {datetime.datetime.strptime(d, '%Y-%m-%d').strftime('%Y-%m') for d in month_start}
                                #if datetime.datetime.strptime(d, '%Y-%m-%d').strftime('%Y-%m') not in ['2020-02', '2020-03', '2020-04', '2020-05', '2020-06'] }  #Remove for covid season

        month_start_end = {}

        for c_month, m_start, m_end in zip(sorted(potential_coverage), month_start, month_end):
            if c_month not in ['2020-02', '2020-03', '2020-04', '2020-05', '2020-06']:
                month_start_end[c_month]={'start':m_start, 'end': m_end}
        self.month_start_end = month_start_end

        return potential_coverage

    def _find_coverage(self) -> set:
        """ Finds the years of data that have been pulled

        Returns:
            coverage (set): set of years that have already been pulled
        """

        #Assumes files are in yyyy.tsv.gz format
        coverage = {(f.split('.')[0]) for f in os.listdir(self.directory_path) if 'tsv' in f}

        return coverage

    def _pull_data(self) -> None:
        """Pulls the data based on years that are currently uncovered using self.func.
           Writes a gzip tsv to self.directory_path/year.tsv.gz
        """

        outstanding_months = sorted(self.potential_coverage - self.coverage, reverse=True)
        logging.info(f"There are {len(outstanding_months)} months to pull, pulling {min([self.limit,len(outstanding_months)])} now")

        for month in outstanding_months[:self.limit]:
            sleep_random()
            try:
                month_path = f"{self.directory_path}{month}.tsv.gz"
                month_start = self.month_start_end.get(month)['start']
                month_end = self.month_start_end.get(month)['end']
                logging.info(f"Pulling data for {month}, month_start={month_start}, month_end={month_end}")
                df = self.func(start_dt=month_start, end_dt=month_end)
                df.to_csv(month_path, index=False, sep='\t', compression='gzip')

                logging.info(f"Wrote data to {month_path}")
            except:
                logging.info(f"Could not pull data for {self.name} from {month}")

    def _aggregate_data(self) -> None:
        """ (DEPRECATED) Aggregates all data in the self.directory_path and writes it to self.path"""

        logging.info(f"Begining to aggreate data and refresh {self.table_path}")
        files = [f for f in os.listdir(self.directory_path) if 'tsv' in f]
        dfs = [pd.read_csv(self.directory_path+f,sep='\t', compression='gzip').drop(columns=['index']) for f in files]
        df = pd.concat(dfs)
        df.to_csv(self.table_path, sep='\t', index=False, compression='gzip')
        logging.info(f"Finished aggregating data and refreshing {self.table_path}")

    def _remove_most_recent_data(self) -> None:
        """Removes the most recent data in self.directory_path to refresh"""

        #Test to see if there there any files that are not in yyyy format
        bad_format = [f for f in os.listdir(self.directory_path) if 'tsv' and len(f.split('.')[0])>7]
        
        if len(bad_format)>0:
            for bad_file in bad_format:
                bad_file_path = f"{self.directory_path}{bad_file}"
                logging.info(f"Removing malformed files: {bad_file_path}")
                os.remove(bad_file_path)


        coverage = {(f.split('.')[0]) for f in os.listdir(self.directory_path) if 'tsv' in f}
        max_date = max(coverage)
        most_recent_path = f"{self.directory_path}{max_date}.tsv.gz"
        os.remove(most_recent_path)
        logging.info(f"Just removed {most_recent_path} to refresh data for {self.name}")

        next_max_date = max(coverage - {max_date})
        next_most_recent_path = f"{self.directory_path}{next_max_date}.tsv.gz"
        os.remove(next_most_recent_path)
        logging.info(f"Just removed {next_most_recent_path} to refresh data for {self.name}")

def pull_single_table(func: Callable[[],pd.DataFrame], path_prefix: str, kwargs=None) -> pd.DataFrame:
    """Pulls data from a pybaseball function that doesn't require an argument
       and overwrites it to to the path_prefix+func.__name__.tsv.gz

    Args:
        func (func): The function that is pulling the data
        path_prefix (str): The prefix for the write path for this table

    Returns:
        None
    """
    source = path_prefix.split('/')[1]

    logging.info(f"Begining to pull {func.__name__} from {source}")

    if kwargs is not None:
        df = func(**kwargs)
    else:
        df = func()
    path = path_prefix+func.__name__+'.tsv.gz'
    df.to_csv(path, index=False, sep='\t', compression='gzip')

    logging.info(f"Pulled {func.__name__} from {source} and wrote it to {path}")

    return df

def main():

    t = MultiYearDataPull(name='amateur_draft', schema ='draft', func=amateur_draft, min_year=1980, limit=10, current_year=True, add_year=True)
    t.update_table()
    

if __name__ == "__main__":
    configure_logging()
    main()
