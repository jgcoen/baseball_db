import logging
import os
import random
import time
from typing import Callable

import pandas as pd
import psycopg2
import yaml


def configure_logging():
    """Configures a basic logger with level=logging.INFO"""
    return logging.basicConfig(level=logging.INFO, format='%(asctime)s  %(name)s - %(levelname)s:%(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')

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
