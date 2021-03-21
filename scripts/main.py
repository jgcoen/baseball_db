import update_db
import logging
from utils import configure_logging

def main():
    # logging.basicConfig(level=logging.DEBUG, format='%(asctime)s  %(levelname)s:%(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
    logging.info('Do Something')
    update_db.main()


if __name__ == "__main__":
    configure_logging()
    main()