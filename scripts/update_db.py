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

        self.path = None
    
    @property
    def _get_file_path(self):
        """ Creates the full path to the table"""
        path = f"data/{schema}/{load_config.get(path)}"
        self.path = path

    #def load_data(self):




def main():
    conn = psycopg2.connect()
    cur = conn.cursor()
    cur.close()
    conn.close()

def test():
    path_prefix = '/Users/James/Documents/data_projects/baseball_db/data/'
    with open('scripts/tables_config.yaml') as f:
        config = yaml.safe_load(f)
    
    
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
    test()
