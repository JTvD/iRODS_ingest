import json
import pandas as pd
from os import path, getenv, listdir
from getpass import getpass
from pathlib import Path
from dotenv import load_dotenv
from time import sleep
from iRODS_ingest.utils import setup_logger
# iBridges instantiates a logger which causes the basic config setting to be ignored
setup_logger()

from ibridges import Session
from ibridges.data_operations import create_collection
from ibridges.util import get_size, obj_replicas, get_dataobject, get_collection
from ibridges.data_operations import upload, download
from ibridges.meta import MetaData
from ibridges.path import IrodsPath
from ibridges.rules import execute_rule


if __name__ == "__main__":
    local_source_folder = r"""G:\G8"""
    irods_target_folder = 'NPEC/home/M4'

    load_dotenv()
    logger = setup_logger()

    ## Setup iBridges
    # Environment file
    print(path.expanduser("~"))
    print(getenv('IRODS_ENV_FILE'))
    env_file = Path(path.expanduser("~")).joinpath(".irods", getenv('IRODS_ENV_FILE'))

    if not path.exists(env_file):
        logger.error('Environment file not found')
        exit()

    # Authenticate
    with open(env_file, "r") as f:
        ienv = json.load(f)
    password = getpass("Your iRODS password")
    session = Session(irods_env=ienv, password=password)
    # 5 hours in seconds, as workaround for the checksum timeouts
    #session.connection_timeout = 18000
    # cache password
    # session.write_pam_password()
    logger.info('Connected to server')

    #target_ipath = IrodsPath(session, f'/{session.zone}/{irods_target_folder}')
    target_ipath = IrodsPath(session) #, f'{session.zone}/{session.username}')
    target_ipath = target_ipath.parent
    target_ipath = target_ipath.joinpath('M4')

    # List data to upload
    file_dict = {}
    for filename in listdir(local_source_folder):
        local_path = path.join(local_source_folder, filename)
        if path.isfile(local_path):
            if path.splitext(filename)[1] not in ['.tar', '.png']:
                logger.info('skipping %s' % filename)
                continue
            # Extract path parameters
            cell = filename.split('_')[0]
            year = filename.split('_')[1][:4]

            #file_dict[local_path] = target_ipath.joinpath(filename)
            file_dict[local_path] = target_ipath.joinpath(cell, year, filename)

    # Upload
    options = {'regChksum': True, 'verifyChksum': True, 'ChksumAll': True}
    for local_path, irods_path in file_dict.items():
        # Check if collections exist
        if not irods_path.parent.collection_exists():
            create_collection(session, irods_path.parent)
            print(f"creating irods collection: {irods_path.parent}")
        # check if data object exists
        #if local_path in [r"W:\To tape\G8_20230320_bigplants.tar", r"W:\To tape\G8_20230510_hl_brassica.tar"]:
        #    continue
        if not irods_path.dataobject_exists():
            logger.info(f"Uploading {local_path} to {irods_path}")
            upload(session, local_path, irods_path, overwrite=True, options=options)

    # # Add metadata
    metada_df = pd.read_excel(path.join(local_source_folder, "export_log.ods"), skiprows=0, engine="odf")
    for i, row in metada_df.iterrows():
        current_ipath = target_ipath.joinpath(row['Climatecell'], str(row['Year']), row['Filename'])
        if not irods_path.dataobject_exists():
            logger.error(f"Adding metadata, data object {current_ipath} not found")
            continue
        do = get_dataobject(session, current_ipath)
        obj_meta = MetaData(do)
        for col in row.keys():
            tagname = f"NPEC_{col}"
            # print(f"{tagname}: {row[col]}")
            if not obj_meta.__contains__(tagname):           
                if str(row[col]) == 'nan':
                    obj_meta.add(tagname, '-')
                else:
                    obj_meta.add(tagname, str(row[col]))
        logger.info(f"Metadata added to {current_ipath}")

        # Triggering tarring
        if not obj_meta.__contains__('archive_status'):
            stdout, stderr = execute_rule(session, rule_file=None, body='rdm_archive_this', params={'*file_or_collection': f'{current_ipath}'})
            if stderr == "" and 'will be tagged.' in stdout:
                logger.info(f"Executed archive rule executed successfully for {current_ipath}")
            else:
                logger.error(f"Archive rule execution failed for {current_ipath}")

            # The metadata tag only becomes visible after refreshing the irods dataobject
            do = get_dataobject(session, current_ipath)
            obj_meta = MetaData(do)
        # check the archiving status
        if obj_meta.__contains__('archive_status'):
            metadata = obj_meta.to_dict()['metadata']
            for key, val, unit in metadata:
                if key == 'archive_status':
                    logger.info(f"Archive rule status for {current_ipath}: {val}")

