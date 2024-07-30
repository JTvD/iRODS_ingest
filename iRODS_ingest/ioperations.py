# iBridges operations
import logging
import multiprocessing
import numpy as np
from datetime import datetime
from pathlib import Path
from ibridges import Session
from ibridges.data_operations import create_collection, upload
from ibridges.meta import MetaData
from ibridges.util import get_dataobject, obj_replicas
from ibridges.rules import execute_rule
from ibridges.path import IrodsPath


def add_metadata(session, row):
    """Add metdata to an irods dataobject
    Args:
        session (ibridges.Session): irods session
        row (dict): metadata to add
    Returns:
        bool: True if successful
    """
    i_path = IrodsPath(session, row['_iPath'])
    if not i_path.dataobject_exists():
        logging.error(f"Adding metadata, data object {i_path} not found")
        return False
    do = get_dataobject(session, i_path)
    obj_meta = MetaData(do)
    for col in row.keys():
        # Skip upload status columns
        if col[0] == '_':
            continue
        tagname = f"NPEC_{col}"
        # print(f"{tagname}: {row[col]}")
        if not obj_meta.__contains__(tagname):
            if str(row[col]) == 'nan':
                obj_meta.add(tagname, '-')
            else:
                obj_meta.add(tagname, str(row[col]))
    logging.info(f"Metadata added to {i_path}")
    return True


def send_to_tape(session, row):
    """Send an irods dataobject to tape
    Args:
        session (ibridges.Session): irods session
        row (dict): metadata of object
    Returns:
        bool: True if successful
    """
    i_path = IrodsPath(session, row['_iPath'])
    do = get_dataobject(session, i_path)
    obj_meta = MetaData(do)
    if not obj_meta.__contains__('archive_status'):
        stdout, stderr = execute_rule(session, rule_file=None,
                                      body='rdm_archive_this',
                                      params={'*file_or_collection': str(i_path)})
        if stderr == "" and 'will be tagged.' in stdout:
            return True
        else:
            return False


def check_status(session, row):
    """check the archiving status of a dataobject
    Args:
        session (ibridges.Session): irods session
        row (dict): metadata of object
    Returns:
        bool: True if archived, false if not
    """
    i_path = IrodsPath(session, row['_iPath'])
    do = get_dataobject(session, i_path)
    status = max(repl[4] for repl in obj_replicas(do))
    if status != 'good':
        # status can be: stale, good, intermediate, write-locked
        logging.info(f"Bad status detected after upload for {i_path}")
        return False
    obj_meta = MetaData(do)
    if obj_meta.__contains__('archive_status'):
        metadata = obj_meta.to_dict()['metadata']
        for key, val, unit in metadata:
            if key == 'archive_status':
                # logging.info(f"Archive rule status for {i_path}: {val}")
                if val == 'completed_and_hot_deleted':
                    return True
    return False


class I_WORKER(multiprocessing.Process):
    """Worker class to upload files to iRODS"""
    def __init__(self, ienv: dict,
                 password: str,
                 stop_worker: multiprocessing.Event,
                 files_to_upload_queue: multiprocessing.Queue,
                 uploaded_queue: multiprocessing.Queue,
                 id: int):
        super().__init__()
        self.ienv = ienv
        self.password = password
        self.upload_options = {'regChksum': True, 'verifyChksum': True, 'ChksumAll': True}
        self.stop_worker = stop_worker
        self.files_to_upload_queue = files_to_upload_queue
        self.uploaded_queue = uploaded_queue
        self.id = id

    def uploader(self, local_path, irods_path):
        if not irods_path.parent.collection_exists():
            create_collection(self.session, irods_path.parent)
            logging.info(f"creating irods collection: {irods_path.parent}")
        # check if data object exists
        if not irods_path.dataobject_exists():
            start_time = datetime.now()
            logging.info(f"Uploading {local_path} to {irods_path}")
            upload(self.session, local_path, irods_path, overwrite=True, options=self.upload_options)
            logging.info(f"Uploader {self.id} uploaded {local_path} in {datetime.now() - start_time}")

        # Check if the file or files in folder are uploaded succesfully
        if irods_path.dataobject_exists():
            self.check_file_status(irods_path)
        elif irods_path.collection_exists():
            files = [file.relative_to(local_path.parent) for file in local_path.rglob('*') if file.is_file()]
            for file in files:
                self.check_file_status(irods_path.joinpath(file))

    def run(self):
        self.session = Session(irods_env=self.ienv, password=self.password)
        while not self.stop_worker.is_set():
            local_path = ""
            row_dict = self.files_to_upload_queue.get()
            if 'NONE' in row_dict.keys() or self.stop_worker.is_set():
                # Sentinel value to indicate the end of the queue
                logging.info("Stopping I_WORKER %d", self.id)
                self.uploaded_queue.put(self.id)
                break
            if not np.isnan(row_dict['_zipPath']):
                local_path = Path(row_dict['_zipPath'])
            else:
                local_path = Path(row_dict['_Path'])
            irods_path = IrodsPath(self.session, row_dict['_iPath'])
            self.uploader(local_path, irods_path)
            self.uploaded_queue.put(str(irods_path))
            #except Exception as e:
            #    logging.error(f"Error uploading file {local_path}: {e}")

    def check_file_status(self, irods_path):
        logging.info(f"Checking status of {irods_path}")
        status = max(repl[4] for repl in obj_replicas(get_dataobject(self.session, irods_path)))
        if status != 'good':
            logging.info(f"Bad status detected after upload for {irods_path}")
            exit(1)
