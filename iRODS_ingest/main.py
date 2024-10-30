from getpass import getpass
from pathlib import Path
import argparse
import logging
import multiprocessing
import pandas as pd
import queue

import utils as utils
from __init__ import FIVE_TB_FILE_LIMIT
# iBridges instantiates a logger which causes the basic config setting to be ignored
utils.setup_logger()
import ioperations as ioperations
from smb import SMB
from helpers import create_task_df, check_paths
from zipper import ZipperProcess
from ibridges import Session


def queue_multipart_zips(to_upload_queue, upload_df, row_dict):
    """Queue multipart zips and add them to the upload_df for status monitoring (parts don't get any metadata)"""
    parts = utils.check_for_multipart_zip(row_dict['_zipPath'])

    # Single zip
    if len(parts) == 1:
        to_upload_queue.put(row_dict)
        return upload_df

    # Multipart zip
    row_dict['_status'] = 'Zipped FF'
    part_dicts = []
    for part in parts:
        # Add part to status dataframe
        if str(part) != row_dict['_zipPath']:
            part_dict = row_dict.copy()
            part_dict['_zipPath'] = str(part)
            part_dict['_iPath'] = row_dict['_iPath'] + f".z{part.suffix[-2:]}"
            part_dict['_size'] = 0
            part_dicts.append(part_dict)
        to_upload_queue.put(part_dict)
    part_df = pd.DataFrame(part_dicts)
    upload_df = pd.concat([upload_df, part_df], ignore_index=True)
    return upload_df


if __name__ == "__main__":
    # Parse arguments
    parser = argparse.ArgumentParser(description="Script to process and upload files.")
    parser.add_argument('--config', type=str, required=False, help='Path to the config file')
    parser.add_argument('-t', '--totape', dest='totape', default=False, required=False, 
                        action="store_true", help='Add this flag to send files to tape')
    args = parser.parse_args()

    # Check and load the config
    if args.config:
        config_file = Path(args.config)
    else:
        config_file = Path(__file__).parent.joinpath("config.json")
    if not utils.check_file_exists(config_file):
        logging.error('Missing config file, exiting')
        exit(1)
    config = utils.load_json(config_file)

    # Prep progress CSV path
    if 'PROGRESS_FILE' in config.keys() and config['PROGRESS_FILE'] and Path(config['PROGRESS_FILE']).parent.is_dir():
        progress_file_path = Path(config['PROGRESS_FILE'])
    else:
        progress_file_path = Path(__file__).parent.joinpath('in_progress.csv')

    # Retreive users password, used to mount the W if desired and login to iRODS
    password = getpass('Your iRODS password')

    # Prep mountpoint (W) if desired
    if config['SMB_MOUNT']:
        smb = SMB(config['SMB'])
        smb.mount_share(password)

    # Check all the paths
    source_path, zip_path, target_ipath, ienv = check_paths(config, password)
    isession = Session(irods_env=ienv, password=password)

    # Check if there is an 'in_progress.csv', if not create it
    # Only uploads the files with a 'v' in the '_to_upload' column
    if Path(__file__).parent.joinpath('in_progress.csv').exists():
        logging.info('Found in_progress.csv, continuing from there')
        to_upload_df = pd.read_csv(progress_file_path)
    else:
        metada_df = pd.read_excel(Path(source_path).joinpath(config['METADATA_EXCEL']),
                                  skiprows=0, engine="openpyxl")
        to_upload_df = metada_df.loc[metada_df['_to_upload'] == 'v'].copy()
        if '_status' not in to_upload_df.columns:
            to_upload_df['_status'] = ""
        to_upload_df['_status'] = to_upload_df['_status'].astype(str)
        to_upload_df = create_task_df(to_upload_df, source_path, target_ipath, zip_path, isession)
        to_upload_df.to_csv(progress_file_path, index=False)

    # Create the shared objects
    ff_to_zip_queue = multiprocessing.Queue()
    zipped_files_queue = multiprocessing.Queue()
    to_upload_queue = multiprocessing.Queue()
    uploaded_queue = multiprocessing.Queue()
    stop_workers = multiprocessing.Event()
    available_diskspace = utils.parse_filesize(config['LOCAL_ZIP_SPACE'])
    zip_processes = {}

    # Loop over the files in the zipped folder, this space is already used...
    for file in Path(config['LOCAL_ZIP_TEMP']).iterdir():
        available_diskspace -= file.stat().st_size

    # Fill the queues with jobs
    for ind, row in to_upload_df.iterrows():
        if row['_status'] == 'existing ipath' or row['_status'] == 'Empty folder':
            logging.info(f"Skipping existing iPath: {row['Foldername']}")
            continue
        # check if folder exists, else: exit program
        if not Path(row['_Path']).exists():
            logging.error(f"Path does not exist {row['_Path']}, index: {ind}")
            exit(1)
        # Only compute the file/folder size if not already done
        if pd.isna(row['_size']):
            ff_size = utils.get_ffsize(row['_Path'])
            row['_size'] = ff_size
            to_upload_df.at[ind, '_size'] = ff_size
            row['_size'] = ff_size
        if row['_status'] == 'Empty folder':
            logging.info(f"Skipping empty folder: {row['Foldername']}")
            continue
        elif row['_status'] in ['Folder', 'Zipped FF'] and config['ZIP_FOLDERS']:
            # Check if the folder is already zipped
            if not pd.isna(row['_zipPath']):
                zip_path = Path(row['_zipPath'])
                if zip_path.exists() and row['_status'] == 'Zipped FF':
                    logging.info(f"Found zip file: {row['_zipPath']}")
                    to_upload_queue.put(row.to_dict())
                    continue
                else:
                    # Partial zip, delete
                    if zip_path.exists():
                        available_diskspace += zip_path.stat().st_size
                        zip_path.unlink()
                        # Multipart zips
                    if zip_path.with_suffix('.z01').exists():
                        for file in zip_path.parent.glob(f"{zip_path.stem}.*"):
                            available_diskspace += file.stat().st_size
                            file.unlink()
                    ff_to_zip_queue.put(row.to_dict())
                # Check if the folder is too large to zip
                if row['_size'] > available_diskspace:
                    logging.error(f"Folder {row['_Path']} is too large: {row['_size']}/{available_diskspace}")
                    exit(1)
        elif row['_status'] == 'Folder' and not config['ZIP_FOLDERS']:
            # 5TB, max file size for the s3 api used by iRODS
            if row['_size'] > FIVE_TB_FILE_LIMIT:
                if config['ZIP_SPLIT_ABOVE_5TB'] and ZipperProcess.get_winrar_path() != "":
                    ff_to_zip_queue.put(row.to_dict())
                else:
                    logging.error(f"Folder {row['_Path']} is too large for the s3api, skipping")
                    row['_status'] == 'Skipped s3 limit'
            else:
                to_upload_queue.put(row.to_dict())
        elif row['_status'] == 'File':
            # 5TB, max file size for the s3 api used by iRODS
            if row['_size'] > FIVE_TB_FILE_LIMIT:
                if config['ZIP_SPLIT_ABOVE_5TB'] and ZipperProcess.get_winrar_path() != "":
                    ff_to_zip_queue.put(row.to_dict())
                else:
                    logging.error(f"Folder {row['_Path']} is too large for the s3api, skipping")
                    row['_status'] == 'Skipped s3 limit'
            else:
                to_upload_queue.put(row.to_dict())
    # Update the progress csv
    to_upload_df.to_csv(progress_file_path, index=False)

    # Add the None jobs to signal the process they are done
    for i in range(0, config['NUM_ZIPPERS']):
        ff_to_zip_queue.put({'NONE': 'NONE'})

    # If zipping is preferred, start the processes
    if config['ZIP_FOLDERS']:
        free_diskspace = multiprocessing.Value('d', available_diskspace)
        disk_space_lock = multiprocessing.Lock()
        for i in range(0, config['NUM_ZIPPERS']):
            zipper = ZipperProcess(stop_workers,
                                   ff_to_zip_queue,
                                   zipped_files_queue,
                                   disk_space_lock,
                                   free_diskspace,
                                   i)
            zipper.start()
            zip_processes[i] = zipper

    # Start the iRODS processes
    i_processes = {}
    for i in range(0, config['NUM_IWORKERS']):
        iworker = ioperations.I_WORKER(ienv, password, stop_workers, to_upload_queue, uploaded_queue, i)
        iworker.start()
        i_processes[i] = iworker

    # Update the progress csv as tasks are completed
    while len(zip_processes) > 0 or len(i_processes) > 0:
        if len(zip_processes) > 0 or zipped_files_queue.qsize() > 0:
            try:
                zipped_dfrow = zipped_files_queue.get(timeout=10)
                if isinstance(zipped_dfrow, int):
                    logging.info(f"Zipper {zipped_dfrow} finished")
                    zip_processes.pop(zipped_dfrow)
                else:
                    row_index = to_upload_df.loc[to_upload_df['_zipPath'] == zipped_dfrow['_zipPath']].index[0]
                    to_upload_df.at[row_index, '_status'] = 'Zipped FF'

                    to_upload_df = queue_multipart_zips(to_upload_queue, to_upload_df, zipped_dfrow)
                    to_upload_df.to_csv(progress_file_path, index=False)
            except queue.Empty:
                pass

        # No new upload jobs expected
        elif len(zip_processes) == 0:
            for i in range(0, config['NUM_IWORKERS']):
                to_upload_queue.put({'NONE': 'NONE'})

        # Uploaders
        try:
            i_path = uploaded_queue.get(timeout=10)
            if isinstance(i_path, int):
                logging.info(f"iWorker {i_path} finished")
                i_processes.pop(i_path)
            else:
                row_index = to_upload_df.loc[to_upload_df['_iPath'] == i_path].index[0]
                to_upload_df.at[row_index, '_status'] = 'Uploaded'
                to_upload_df.to_csv(progress_file_path, index=False)
                # Cleanup the zip file if it was created
                if not pd.isna(to_upload_df.at[row_index, '_zipPath']):
                    if Path(to_upload_df.at[row_index, '_zipPath']).exists():
                        Path(to_upload_df.at[row_index, '_zipPath']).unlink()
                        with disk_space_lock:
                            free_diskspace.value += to_upload_df.at[row_index, '_size']
        except queue.Empty:
            pass
    logging.info("All workers finished, proceeding with metadata")

    # Add metadata
    for ind, row in to_upload_df.iterrows():
        if row['_status'] == 'Uploaded':
            ioperations.add_metadata(isession, row)
            to_upload_df.at[ind, '_status'] = 'Metadata added'
    to_upload_df.to_csv(progress_file_path, index=False)

    # Send to tape
    if args.totape:
        for ind, row in to_upload_df.iterrows():
            if row['_status'] == 'Metadata added':
                if ioperations.send_to_tape(isession, row):
                    to_upload_df.at[ind, '_status'] = 'Sent to tape'
        to_upload_df.to_csv(progress_file_path, index=False)

    # Check taping status
    for ind, row in to_upload_df.iterrows():
        if row['_status'] == 'Sent to tape':
            if ioperations.check_status(isession, row):
                to_upload_df.at[ind, '_status'] = 'Archived'
    to_upload_df.to_csv(progress_file_path, index=False)

    # Print the summary of the statuses
    status_counts = to_upload_df['_status'].value_counts()
    logging.info(status_counts)
