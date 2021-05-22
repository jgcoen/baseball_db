import logging
import os
import random
import time

import pandas as pd
import psycopg2
import yaml


def configure_logging():
    """Configures a basic logger with level=logging.INFO"""
    logging.getLogger().setLevel(logging.INFO)


def load_secrets(path: str='secrets.yaml') -> None:
    """Exports secrets configured in ../secrets.yaml as environment variables

    Args:
        path (str): Path to secrets yaml. Default = secrets.yaml

    Returns:
        None
    """

    with open(path, 'r') as file:
        secrets = yaml.safe_load(file)['secrets']

    for k, v in secrets.items():
        os.environ[k] = v

def sleep_random(min_seconds: float=1, max_seconds: float=5) -> None:
    """Function that waits at a random time based on a uniform distribution

    Args:
        min_seconds (float): Minimum number of seconds to wait. (default is 1)
        max_seconds (float): Maximum number of seconds to wait. (default is 5)

    Returns:
        None

    """
    wait = random.uniform(min_seconds,max_seconds)
    time.sleep(wait)

def create_db_connection():

    load_secrets()
    conn = psycopg2.connect(os.getenv('db_access'))
    cur = conn.cursor()

    return conn, cur

def check_statcast_schema(data_path='data/statcast/statcast_dir/'):
    
    file_list = sorted([data_path+f for f in os.listdir(data_path) if 'tsv.gz' in f])
    
    d = {}
    for f in file_list:
        df = pd.read_csv(f, sep='\t')

        if 'index' in df.columns:
            print(f'{f} has the index column')
            df = df.drop(columns='index')
            df.to_csv(f, index=False, sep='\t', compression='gzip')
            print(f'Wrote the updated df to {f}')
        else:
            print(f'{f} does not have the index column, passing')
