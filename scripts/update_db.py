import pandas as pd
import psycopg2
import yaml
from typing import List, Mapping
from utils import load_secrets, create_db_connection, configure_logging
import os
import logging

logger = logging.getLogger(__name__)





class Table():

    def __init__(self, name: str, schema: str, load_config: Mapping[str, str], schema_config: Mapping[str, str]) -> None:
        self.name = name
        self.schema = schema
        self.table_name = f"{schema}.{name}"
        self.load_config = load_config
        self.schema_config = schema_config
        self.path = self._get_file_path()
        self.schema_string = self._create_schema()
        self.create_statement = self._create_create_statement()
        self.drop_statement = self._create_drop_statement()
        self.copy_statement = self._create_copy_statement()


        #Only call if necessary, could be expensive
        self.df = None

    def _get_file_path(self):
        """ Creates the full path to the table"""

        relative_path = f"data/{self.schema}/{self.load_config.get('path')}"
        path = os.path.abspath(relative_path)
        
        return path

    def _load_data(self):
        """Loads the data using pd.read_csv"""

        df = pd.read_csv(self.path, **self.load_config)

        self.df = df

        return None

    def _create_schema(self):

        col_strings = []
        for col_name, col_dtype in self.schema_config.items():
            col_string = f"{col_name} {col_dtype}"
            col_strings.append(col_string)

        schema_string = f"({', '.join(col_strings)})"

        return schema_string

    def _create_create_statement(self):

        create_statement = f"create table if not exists {self.table_name} {self.schema_string}"

        return create_statement

    def _create_drop_statement(self):

        drop_statement = f"drop table if exists {self.table_name}"

        return drop_statement
    
    def _create_copy_statement(self):

        if self.load_config.get('compression',0)=='gzip':
            from_statement = f"gzip -dc {self.path}"
        else:
            from_statement = self.path

        copy_statement = f'''copy {self.table_name}
                            from program '{from_statement}' 
                            CSV Header DELIMITER E'\t';'''

        return copy_statement

    def update_table(self,conn,cur):

        logger.info(f"Beggining to update {self.table_name}")
        #Drop Table
        cur.execute(self.drop_statement)
        conn.commit()

        #Create Table
        cur.execute(self.create_statement)
        conn.commit()

        #Copy Table
        cur.execute(self.copy_statement)
        conn.commit()

        logger.info(f"Finished updating {self.table_name}")

        return None

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
    conn, cur = create_db_connection()
    configure_logging()

    table_configs = load_config()

    for s in table_configs:
        for t in table_configs[s]:
            config = table_configs[s][t]
            table = Table(name=t, schema=s, load_config=config.get('load'), schema_config=config.get('schema'))
            table.update_table(conn, cur)
           

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
