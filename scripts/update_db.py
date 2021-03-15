import pandas as pd
import psycopg2
import yaml
from typing import List, Mapping



class Table():

    def __init__(self, name: str, schema: str, load_config: Mapping[str, str], schema_config: Mapping[str, str]) -> None:
        self.name = name
        self.schema = schema
        self.load_config = load_config
        self.schema_config = schema_config
        self.path = self._get_file_path()
        self.df = self._load_data()
        self.schema_string = self._create_schema()

        self._create_create_statement()

    def _get_file_path(self):
        """ Creates the full path to the table"""

        path = f"data/{self.schema}/{self.load_config.get('path')}"
        return path

    def _load_data(self):
        """Loads the data using pd.read_csv"""

        self.load_config.pop('path')
        df = pd.read_csv(self.path, **self.load_config )

        return df
    
    def _create_schema(self):

        col_strings = []
        for col_name, col_dtype in self.schema_config.items():
            col_string = f"{col_name} {col_dtype}"
            col_strings.append(col_string)
        
        schema_string = f"({', '.join(col_strings)})"

        return schema_string

    def _create_create_statement(self):

        create_statement = f"create table {self.schema}.{self.name} {self.schema_string};"

        print(create_statement)

def load_config(path: str='scripts/tables_config.yaml') -> Mapping[str, str]:
    """Loads a yaml config from the specified path

    Args:
        path (str): Path of the config

    Returns:
        config (Mapping): The dictionary from the loaded yaml

    """

    with open(path) as f:
        config = yaml.safe_load(f)

    return config


def main():

    table_configs = load_config()

    for s in table_configs:
        for t in table_configs[s]:
            config = table_configs[s][t]
            table = Table(name=t, schema=s, load_config=config.get('load'), schema_config=config.get('schema'))

    # conn = psycopg2.connect()
    # cur = conn.cursor()
    # cur.close()
    # conn.close()

if __name__ == "__main__":
    main()
