import random
import time
import os
from typing import Callable
import logging
import yaml
import pandas as pd

def configure_logging():
    """Configures a basic logger with level=logging.INFO"""
    return logging.basicConfig(level=logging.INFO)

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

def pull_single_table(func: Callable[[],pd.DataFrame], path_prefix: str) -> pd.DataFrame:
    """Pulls data from a pybaseball function that doesn't require an argument
       and overwrites it to to the path_prefix+func.__name__.tsv.gz

    Args:
        func (func): The function that is pulling the data
        path_prefix (str): The prefix for the write path for this table

    Returns:
        None
    """
    source = path_prefix.split('/')[1]

    logging.info(f"Begining to pull {func.__name__} from {source}")

    df = func()
    path = path_prefix+func.__name__+'.tsv.gz'
    df.to_csv(path, index=False, sep='\t', compression='gzip')

    logging.info(f"Pulled {func.__name__} from {source} and wrote it to {path}")

    return df
