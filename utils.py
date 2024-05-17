import logging
import pandas as pd
import openpyxl


def setup_logging():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    c_handler = logging.StreamHandler()
    c_handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s',datefmt='%Y-%m-%d %H:%M:%S')
    c_handler.setFormatter(formatter)
    logger.addHandler(c_handler)
    return logger


def write_to_csv(df:pd.DataFrame, name, index=False) -> None:
    logger = setup_logging()
    if not name: raise ValueError('No name provided.')
    if not isinstance(name,str): raise TypeError('Incorrect data type provided for file name.')

    df.to_csv(f'output/{name}', index=index)
    logger.debug(f'Completed writing {name} to CSV file. Check output/{name}.')

def read_from_excel(file_name:str) -> str:
    df = pd.read_excel(file_name)
    return df