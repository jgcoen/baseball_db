import logging
import os

import pandas as pd
import yaml
from utils import configure_logging, create_db_connection


class Table():

    def __init__(self, name: str, schema: str, load_config: dict, schema_config: dict) -> None:
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

    def _get_file_path(self) -> str:
        """Creates the full path to the table"""

        relative_path = f"data/{self.schema}/{self.load_config.get('path')}"
        path = os.path.abspath(relative_path)

        return path

    def _load_data(self) -> None:
        """Loads the data using pd.read_csv (Optional)"""

        df = pd.read_csv(self.path, **self.load_config)

        self.df = df

        return None

    def _create_schema(self) -> str:
        """Creates the schema string statement from self.schema_config

        Returns:
            schema_string (str)

        Ex Output:
            (playerID varchar, yearID float, gameNum float, gameID varchar,
             lgID varchar, teamID varchar, GP float, startingPos float)
        """
        conversion_dict =  {'float64': 'float',
                            'int64': 'float',
                            'object': 'varchar'}

        df = pd.read_csv(self.path, sep='\t', compression='gzip', low_memory=False, nrows=40000)
        schema_dict = {c: conversion_dict[str(df[c].dtype)] for c in df.columns}

        col_strings = []
        for col_name, col_dtype in schema_dict.items():
            col_string = f'''"{col_name.lower().replace('.','_')}" {col_dtype}'''
            col_strings.append(col_string)

        schema_string = f"({', '.join(col_strings)})"

        return schema_string

    def _create_create_statement(self) -> str:
        """Creates the create table string command

        Returns:
            create_statement (str)

        Ex Output:
            create table if not exists lahman.all_star_full
            (playerID varchar, yearID float, gameNum float,
             gameID varchar, lgID varchar, teamID varchar, GP float, startingPos float)
        """

        create_statement = f"create table if not exists {self.table_name} {self.schema_string}"

        return create_statement

    def _create_drop_statement(self) -> str:
        """Creates the drop table statement

        Returns:
            drop_statement (str)

        Ex Output:
            drop table if exists lahman.all_star_full
        """

        drop_statement = f"drop table if exists {self.table_name}"
        return drop_statement

    def _create_copy_statement(self) -> str:
        """Creates the copy statement

        Returns:
            copy_statement (str)

        Default functionality is a tsv with a header
            CSV Header DELIMITER E'     '

        If the file being uploaded is a gzip file, instead of
            from 'path'
        the from portion of the statment becomes
            from program 'gzip -dc path'

        Ex Output:
            copy lahman.all_star_full
            from program 'gzip -dc
                /Users/James/Documents/data_projects/baseball_db/data/lahman/all_star_full.tsv.gz'
            CSV Header DELIMITER E'     ';
        """

        if self.load_config.get('compression',0)=='gzip':
            from_statement = f"gzip -dc {self.path}"
        else:
            from_statement = self.path

        copy_statement = f'''copy {self.table_name}
                            from program '{from_statement}' 
                            CSV Header DELIMITER E'\t';'''

        return copy_statement

    def update_table(self, conn, cur) -> None:
        """Drops, creates, and copies data into database

        Args:
            conn: database connection
            cur: database cursor
        """

        logging.info(f"Begining to update {self.table_name}")
        #Drop Table
        cur.execute(self.drop_statement)
        conn.commit()

        #Create Table
        cur.execute(self.create_statement)
        conn.commit()

        #Copy Table
        cur.execute(self.copy_statement)
        conn.commit()

        logging.info(f"Finished updating {self.table_name}")

        return None

def load_config(path: str='scripts/tables_config.yaml') -> dict:
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

    logging.info('Begining to update the database')

    conn, cur = create_db_connection()
    config = load_config()

    for schema, schema_config in config.items():
        for table, table_config in schema_config.items():
            t = Table(name=table, schema=schema, load_config=table_config.get('load'), schema_config=table_config.get('schema'))
            t.update_table(conn,cur)

    cur.close()
    conn.close()

    logging.info('Finished updating the database')

if __name__ == "__main__":
    configure_logging()
    main()
