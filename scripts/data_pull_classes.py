import datetime
import logging
import os

import numpy as np
import pandas as pd
from pybaseball import statcast
from utils import configure_logging, sleep_random


class MultiYearDataPull:

    def __init__(self, name, schema, func, min_year, limit: int=10):
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

    def _find_potential_coverage(self) -> set:
        """ Finds the potential years of data to pull, using the min_year

        Returns:
            potential_coverage (set): set of years that could potentially be pulled
        """

        current_year = datetime.datetime.today().year
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
                df = self.func(year)
                df.to_csv(year_path, index=False, sep='\t', compression='gzip')

                logging.info(f"Wrote data to {year_path}")
            except:
                logging.info(f"Could not pull data for {self.name} from {year}")

    def _aggregate_data(self) -> None:
        """ Aggregates all data in the self.directory_path and writes it to self.path"""

        logging.info(f"Begining to aggreate data and refresh {self.table_path}")
        files = [f for f in os.listdir(self.directory_path) if 'tsv' in f]
        dfs = [pd.read_csv(self.directory_path+f,sep='\t', compression='gzip') for f in files if 'tsv' in f]
        df = pd.concat(dfs)
        df.to_csv(self.table_path, sep='\t', index=False, compression='gzip')
        logging.info(f"Finished aggregating data and refreshing {self.table_path}")

    def _remove_most_recent_data(self) -> None:
        """Removes the most recent data in self.directory_path to refresh"""

        coverage = {int(f.split('.')[0]) for f in os.listdir(self.directory_path) if 'tsv' in f}

        if coverage == {}:
            max_year = max(coverage)
            most_recent_path = f"{self.directory_path}{max_year}.tsv.gz"
            os.remove(most_recent_path)

            logging.info(f"Just removed {most_recent_path} to refresh data for {self.name}")
        else:
            logging.info('There is no data to remove')


    def update_table(self):
        """Updates the table

           Steps
            - remove the most recent data
            - determine current coverage
            - pull data
            - aggregate the data in the directory to the self.path
        """

        logging.info(f"Begining to pull update the data for {self.name}")

        #Remove Most Recent Data
        self._remove_most_recent_data()

        #Coverage
        self.potential_coverage = self._find_potential_coverage()
        self.coverage = self._find_coverage()

        self._pull_data()
        self._aggregate_data()

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

        month_start_end = {}

        for c_month, m_start, m_end in zip(sorted(potential_coverage), month_start, month_end):
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
        """ Aggregates all data in the self.directory_path and writes it to self.path"""
        #Need to drop the index

        logging.info(f"Begining to aggreate data and refresh {self.table_path}")
        files = [f for f in os.listdir(self.directory_path) if 'tsv' in f]
        dfs = [pd.read_csv(self.directory_path+f,sep='\t', compression='gzip').drop(columns=['index']) for f in files]
        df = pd.concat(dfs)
        df.to_csv(self.table_path, sep='\t', index=False, compression='gzip')
        logging.info(f"Finished aggregating data and refreshing {self.table_path}")

    def _remove_most_recent_data(self) -> None:
        """Removes the most recent data in self.directory_path to refresh"""

        coverage = {(f.split('.')[0]) for f in os.listdir(self.directory_path) if 'tsv' in f}
        max_date = max(coverage)
        most_recent_path = f"{self.directory_path}{max_date}.tsv.gz"
        os.remove(most_recent_path)
        logging.info(f"Just removed {most_recent_path} to refresh data for {self.name}")

        next_max_date = max(coverage - {max_date})
        next_most_recent_path = f"{self.directory_path}{next_max_date}.tsv.gz"
        os.remove(next_most_recent_path)
        logging.info(f"Just removed {next_most_recent_path} to refresh data for {self.name}")


def main():

    o = StatcastDataPull(limit=12)
    # print(o.update_table())
    o._remove_most_recent_data()
if __name__ == "__main__":
    configure_logging()
    main()
