import logging
import numpy as np
import pandas as pd
from pathlib import Path
from ibridges import Session
from ibridges.path import IrodsPath

import utils as utils


def check_paths(config: dict, password: str):
    # Check iRODS environment file
    env_file = Path("~").expanduser().joinpath(".irods", config['IRODS_ENV_FILE'])
    if not env_file.exists() and env_file.is_file():
        logging.error('Environment file not found')
        exit(1)

    # Check if source paths exist
    source_path = Path(config['LOCAL_SOURCE_PATH'])
    if not source_path.exists() and not source_path.is_dir():
        logging.error('Source path does not exist')
        exit(1)

    # If zipping check if the zip path exists
    zip_path = ""
    if config['ZIP_FOLDERS']:
        zip_path = Path(config['LOCAL_ZIP_TEMP'])
        if not zip_path.exists() or not zip_path.is_dir():
            logging.error("Zip path does not exist")
            exit(1)

    # Check if target path exists
    target_path = Path(config['IRODS_TARGET_PATH'])
    ienv = utils.load_json(env_file)
    isession = Session(irods_env=ienv, password=password)
    # Verification if a connection is made
    isession.server_version
    # ---------------------------------------------------------
    # Bugged, will not return absolute path
    # target_ipath = IrodsPath(isession, target_path)
    # ---------------------------------------------------------
    target_ipath = IrodsPath(isession)
    if not target_ipath.collection_exists():
        logging.error('Target path does not exist')
        exit(1)
    return source_path, zip_path, target_ipath, ienv


def create_task_df(to_upload_df: pd.DataFrame, source_path: Path, target_ipath: Path, zip_path: Path):
    """ Create a task dataframe:
    Note, folder paths are incomplete, the zipper adds the missing parts
    Args:
        to_upload_df: pd.DataFrame
            DataFrame containing the metadata
        source_path: Path
            Path to the source folder
        target_ipath: Path
            Path to the target folder
        zip_path: Path
            Path to the zip folder
    Returns:
        to_upload_df: pd.DataFrame
            Added fields: _Path, _status, _zipPath, _iPath, _size
    """
    to_upload_df['_zipPath'] = np.nan
    to_upload_df['_size'] = np.nan
    for ind, row in to_upload_df.iterrows():
        local_path = source_path.joinpath(row['Foldername'])
        if row['NPEC module'] == 'Greenhouse':
            ipath = target_ipath.joinpath('M5', row['System'], str(row['Year']))
        elif row['NPEC module'] == 'OpenField':
            ipath = target_ipath.joinpath('M6', row['System'], str(row['Year']))
        else:
            logging.error(f"Unknown NPEC module: {row['NPEC module']} for file: {row['Foldername']}")

        to_upload_df.at[ind, '_Path'] = str(local_path)
        if local_path.is_dir():
            # Check if the folder is empty
            if not any(local_path.iterdir()):
                to_upload_df.at[ind, '_status'] = 'Empty folder'
            else:
                to_upload_df.at[ind, '_status'] = 'Folder'
                if zip_path != "":
                    to_upload_df.at[ind, '_zipPath'] = str(zip_path.joinpath(local_path.name + ".zip"))
                    to_upload_df.at[ind, '_iPath'] = str(ipath.joinpath(local_path.name + ".zip"))
                else:
                    to_upload_df.at[ind, '_iPath'] = str(ipath)
        elif local_path.is_file():
            to_upload_df.at[ind, '_status'] = 'File'
            to_upload_df.at[ind, '_iPath'] = str(ipath.joinpath(local_path.name))
    return to_upload_df
