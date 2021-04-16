import logging
import os

import pandas as pd
import yaml
from utils import configure_logging, create_db_connection


class Table():

    def __init__(self, name: str, schema: str, config: dict) -> None:
        self.name = name
        self.schema = schema
        self.table_name = f"{schema}.{name}"
        self.config = config
        self.relative_path = self.config.get('path')
        self.compression = self.config.get('compression', 'gzip')
        self.sep = self.config.get('sep', '\t')
        self.path = self._get_file_path()
        self.index_statement = self.config.get('index_statement', None)
        self.schema_string = self._create_schema()
        self.create_statement = self._create_create_statement()
        self.drop_statement = self._create_drop_statement()
        self.copy_statement = self._create_copy_statement()


    def _get_file_path(self) -> str:
        """Creates the full path to the table"""

        relative_path = f"data/{self.schema}/{self.relative_path}"
        path = os.path.abspath(relative_path)

        return path

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

        df = pd.read_csv(self.path, sep=self.sep, compression = self.compression, low_memory=False, nrows=40000)
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

        if self.compression=='gzip':
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

        if self.index_statement is not None:
            logging.info(self.index_statement)
            cur.execute(self.index_statement)
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

            if table_config.get('table_type') == 'partitioned':
                t = PartitionedTable(name=table, schema=schema, config=table_config)
            else:
                t = Table(name=table, schema=schema, config=table_config)
            
            t.update_table(conn,cur)

    cur.close()
    conn.close()

    logging.info('Finished updating the database')

class PartitionedTable():

    def __init__(self, name: str, schema: str, config: dict) -> None:
        self.name = name
        self.schema = schema
        self.table_name = f"{schema}.{name}"
        self.config = config
        self.partitioned_by = self.config.get('partitioned_by')
        self.iterator = self.config.get('iterator')
        self.directory = f"data/{schema}/{self.config.get('directory')}/"
        self.compression = self.config.get('compression', 'gzip') #Default is gzip
        self.sep = self.config.get('sep', '\t') #Default is tab
        self.schema_override = self.config.get('schema_override', None)
        self.files = self.gather_files()
        self.schema_string = self._create_schema()
        self.main_create_statement = self._main_create_create_statement()
        self.drop_statement = self._create_drop_statement()

    def gather_files(self):

        files = [self.directory+f for f in os.listdir(self.directory) if 'tsv.gz' in f]

        return files

    def _create_schema(self):
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

        df = pd.read_csv(sorted(self.files)[-1], sep=self.sep, compression=self.compression, low_memory=False, nrows=40000)
        schema_dict = {c: conversion_dict[str(df[c].dtype)] for c in df.columns}

        if self.schema_override is not None:
            for column, dtype in self.schema_override.items():
                schema_dict[column] = dtype

        col_strings = []
        for col_name, col_dtype in schema_dict.items():
            col_string = f'''"{col_name.lower().replace('.','_')}" {col_dtype} null'''
            col_strings.append(col_string)
        schema_string = f"({', '.join(col_strings)})"

        return schema_string

    def _main_create_create_statement(self) -> str:
        """Creates the create table string command

        Returns:
            create_statement (str)

        Ex Output:
            create table if not exists lahman.all_star_full
            (playerID varchar, yearID float, gameNum float,
             gameID varchar, lgID varchar, teamID varchar, GP float, startingPos float)
        """

        create_statement = f"create table if not exists {self.table_name} {self.schema_string} partition by range ({self.partitioned_by})"

        return create_statement

    def _partition_create_statement(self, partition):

        if self.iterator == 'month':
            # Iterator comes in as '2020-04'
            min_range = f"{partition}-01"  #First day of the month
            max_range = pd.date_range(min_range, periods=2, freq='MS')[-1].strftime('%Y-%m-%d') #First day of the next month (range is exclusive)
            partition_name = self._create_partition_name(partition)
            create_statement = f'''
                                create table if not exists {partition_name} 
                                partition of {self.table_name} 
                                for values from ('{min_range}') to ('{max_range}')
                            '''
        elif self.iterator == 'year':
            # Iterator comes as '2008' NOTE We need to convert to integer, since the datatype will be an integer
            min_range = int(partition)
            max_range = int(partition)+1
            partition_name = self._create_partition_name(partition)

            create_statement = f'''
                                create table if not exists {partition_name} 
                                partition of {self.table_name} 
                                for values from ({min_range}) to ({max_range})
                            '''

        elif self.iterator == 'year_date':
            # Iterator comes as '2008', but the partitioned_by is a string of dates YYYY-MM-DD
            min_range = f"{partition}-01-01"
            max_range = f"{int(partition)+1}-01-01"
            partition_name = self._create_partition_name(partition)

            create_statement = f'''
                                create table if not exists {partition_name} 
                                partition of {self.table_name} 
                                for values from ('{min_range}') to ('{max_range}')
                            '''
 
        elif self.iterator == 'year_date_int':
            # Iterator comes as '2008', but the partitioned_by is a date as an int YYYYMMDD
            min_range = int(f"{partition}0101")
            max_range = int(f"{int(partition)+1}0101")
            partition_name = self._create_partition_name(partition)

            create_statement = f'''
                                create table if not exists {partition_name} 
                                partition of {self.table_name} 
                                for values from ('{min_range}') to ('{max_range}')
                            '''
        return create_statement

    def _partition_copy_statement(self, path, partition):

        partition_name = self._create_partition_name(partition)

        if self.compression=='gzip':
            from_statement = f"gzip -dc {path}"
        else:
            from_statement = path

        copy_statement = f'''copy {partition_name}
                            from program '{from_statement}' 
                            CSV Header DELIMITER E'\t';'''

        return copy_statement

    def _create_partition_name(self, partition):

        if self.iterator == 'month':
            #Comes in 2021-04 format
            partition_name = f"{self.schema}.{self.name}_{partition.split('-')[0]}_{partition.split('-')[1]}"
        if self.iterator in ['year', 'year_date', 'year_date_int']:
            # Comes as '2008'
            partition_name = f"{self.schema}.{self.name}_{partition}"

        return partition_name

    def _create_drop_statement(self) -> str:
        """Creates the drop table statement

        Returns:
            drop_statement (str)

        Ex Output:
            drop table if exists lahman.all_star_full
        """

        drop_statement = f"drop table if exists {self.table_name}"

        return drop_statement

    def update_table(self, conn, cur):
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
        cur.execute(self.main_create_statement)
        conn.commit()

        for file in sorted(self.files, reverse=True):
            #We want to create a partition
            partition = file.split('.')[0].split('/')[-1]
            partition_create_statement = self._partition_create_statement(partition)
            cur.execute(partition_create_statement)
            conn.commit()

            #copying
            path = os.path.abspath(file)
            partition_copy_statement = self._partition_copy_statement(path, partition)
            cur.execute(partition_copy_statement)
            conn.commit()

        return None

if __name__ == "__main__":
    configure_logging()
    main()
