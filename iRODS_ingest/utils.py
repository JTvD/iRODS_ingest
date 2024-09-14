import sys
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime
from json import load
from pathlib import Path
import re


def check_file_exists(file_path):
    """ Check if a file exists """
    if not Path(file_path).exists():
        logging.error(f"File {file_path} does not exist")
        return False
    return True


def load_json(config_file):
    """ Loads a config.json and returns the content """
    with open(config_file, 'r', encoding='UTF-8') as file:
        config = load(file)
    return config


def parse_filesize(sizestr: str):
    """Convert human readable filesize to machine filesize"""
    units = {"B": 1, "KB": 2**10, "MB": 2**20, "GB": 2**30, "TB": 2**40,
             "":  1, "KIB": 10**3, "MIB": 10**6, "GIB": 10**9, "TIB": 10**12}
    m = re.match(r'^([\d\.]+)\s*([a-zA-Z]{0,3})$', str(sizestr).strip())
    number, unit = float(m.group(1)), m.group(2).upper()
    return int(number*units[unit])


def get_ffsize(file_folder_path: str):
    """Get the size of a file or folder in bytes"""
    if Path(file_folder_path).is_dir():
        return get_folder_size(file_folder_path)
    return Path(file_folder_path).stat().st_size


def get_folder_size(folder_path: str):
    """Get the size of a folder"""
    total_size = 0
    start_time = datetime.now()
    for path in Path(folder_path).rglob('*'):
        if path.is_file() and not path.is_symlink():
            total_size += path.stat().st_size
    logging.info(f"Computed size of {folder_path} in {datetime.now() - start_time}")
    return total_size


def check_for_multipart_zip(zip_path: str):
    """Check if a zip is multipart and return the partnumbers if there are any"""
    path = Path(zip_path)
    parts = []
    for file in path.parent.glob(f"{path.stem}.*"):
        parts.append(file)
    return parts


def setup_logger(filename='iRODS_upload'):
    ''' setup the logger '''
    cfd = Path(__file__).parent
    log_folder = cfd.joinpath('logs')
    if not log_folder.exists() and not log_folder.is_dir():
        log_folder.mkdir(parents=True, exist_ok=True)

    # initialize the log file
    log_file = log_folder / (filename + '.log')
    log_format = '[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s'
    handlers = [RotatingFileHandler(log_file, 'a', 1000000, 1), logging.StreamHandler(sys.stdout)]
    logging.basicConfig(format=log_format, level=logging.INFO, handlers=handlers)

    # Indicate the start of a new session
    with open(log_file, 'a') as f:
        f.write('\n\n')
        underscores = ''
        for x in range(0, 50):
            underscores = underscores + '_'
        underscores = underscores + '\n'
        f.write(underscores)
        f.write(underscores)
        f.write(f"\t\t {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(underscores)
        f.write(underscores)

    # Return logger
    logger = logging.getLogger('main')
    return logger
