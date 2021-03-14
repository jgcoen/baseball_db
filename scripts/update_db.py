import pandas as pd
import psycopg2
import yaml
from typing import Mapping



class Table():

    def __init__(self, name: str, schema: str, load_config: Mapping[str, str], schema_config: Mapping[str, str]) -> None:
        self.name = name
        self.schema = schema
        self.load_config = load_config
        self.schema_config = schema_config
        self.path = self._get_file_path()
        self.df = self._load_data()


    def _get_file_path(self):
        """ Creates the full path to the table"""

        path = f"data/{self.schema}/{self.load_config.get('path')}"
        return path

    def _load_data(self):
        """Loads the data using pd.read_csv"""

        self.load_config.pop('path')
        df = pd.read_csv(self.path, **self.load_config )

        return df

    #def load_data(self):




def main():

    with open('scripts/tables_config.yaml') as f:
        table_configs = yaml.safe_load(f)

    for s in table_configs:
        for t in table_configs[s]:
            config = table_configs[s][t]
            table = Table(name=t, schema=s, load_config=config.get('load'), schema_config=config.get('schema'))

            print(table.df.head())

    # conn = psycopg2.connect()
    # cur = conn.cursor()
    # cur.close()
    # conn.close()

def test():
    path_prefix = '/Users/James/Documents/data_projects/baseball_db/data/'
    
    
    
    for schema in config:
        for table in config[schema]:
            full_path = path_prefix + schema + '/' + table
            


def create_schema():
    df = pd.read_csv('/Users/James/Documents/data_projects/baseball_db/data/lahman/all_star_full.tsv', sep='\t')
    
    conversion_dict = {'object': 'varchar'
                        
                        
                        }

    dtypes = pd.DataFrame(df.dtypes, columns=['dtype'])
    for e in dtypes.itertuples():
        print(e.dtype=='object')
    

if __name__ == "__main__":
    main()
