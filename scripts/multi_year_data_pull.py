import logging
import pandas as pd
from utils import configure_logging, sleep_random
import datetime
import numpy as np
import os


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
        coverage = {int(f.split('.')[0]) for f in os.listdir(self.directory_path)}

        return coverage

    def _pull_data(self) -> None:
        """Pulls the data based on years that are currently uncovered using self.func. 
           Writes a gzip tsv to self.directory_path/year.tsv.gz
        """

        outstanding_years = sorted(self.potential_coverage - self.coverage, reverse=True)
        logging.info(f"There are {len(outstanding_years)} years to pull, pulling {min([self.limit,len(outstanding_years)])} now")

        for year in outstanding_years[:self.limit]:
            sleep_random()
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
        dfs = [pd.read_csv(self.directory_path+f,sep='\t', compression='gzip') for f in files]
        df = pd.concat(dfs)
        df.to_csv(self.table_path, sep='\t', index=False, compression='gzip')
        logging.info(f"Finished aggregating data and refreshing {self.table_path}")

    def _remove_most_recent_data(self) -> None:
        """Removes the most recent data in self.directory_path to refresh"""

        coverage = {int(f.split('.')[0]) for f in os.listdir(self.directory_path)}
        max_year = max(coverage)
        most_recent_path = f"{self.directory_path}{max_year}.tsv.gz"
        os.remove(most_recent_path)

        logging.info(f"Just removed {most_recent_path} to refresh data for {self.name}")


    def update_table(self):
        """Updates the table

           Steps
            - remove the most recent data
            - determine current coverage
            - pull data
            - aggregate the data in the directory to the self.path
        """

        #Remove Most Recent Data
        self._remove_most_recent_data()

        #Coverage
        self.potential_coverage = self._find_potential_coverage()
        self.coverage = self._find_coverage()

        self._pull_data()
        self._aggregate_data()


def main():

    # o = MultiYearDataPull('season_game_logs', 'retrosheet')
    # print(o)
    print(1)

if __name__ == "__main__":
    configure_logging()
    main()
