import pandas as pd
import psycopg2
import yaml


class Table():

    def __init__(self, name, file_path,schema_config):
        self.name = name
        self.file_path = file_path
        self.schema_config = self.schema
    


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
