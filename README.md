# iRODS ingest
Configurable script to move data to iRODS/tape.
It also adds the metadata provided in the Excel file.

#TODO: finish readme.md

## Important notes:
- issue with irods path '/zone/home/xx' instead of '/zone/home/username'
- can only send files, or zipped folders, to tape
- metadata is set with a 'NPEC_' prefix

## Preperation
- irods json

## Config
A config file is used to pass all the parameters, the goal is to make it easy to repurpose the code:
```
{
    "SMB_MOUNT": true, # Used to automatically mount an SMB mountable disk like the W (isilon) at WUR
    "ZIP_FOLDERS": true, # Zip the folders before uploading, this is advised when sending data to the tape archive
    "TO_TAPE": true, # Wether or not to trigger the archive rule to move the data fromdisk to tape after uploading
    "NUM_ZIPPERS": 1, # Num of zip processes
    "NUM_IWORKERS": 1, # Numer of irods uploaded processes
    "SMB": {
        "SMB_USER": "<user>", # SMB username
        "SMB_PATH":"\\\\fs02mixedsmb.wurnet.nl\\TPE-STANDARD_PROJECTS$\\PROJECTS~NPEC_climaterooms\\", # Example path
        "SMB_LETTER": "W" # Device letter to mount the disk to
    },
    "IRODS_ENV_FILE": "irods_environment.json",
    "LOCAL_SOURCE_PATH": "C:\\iRODS_ingest\\test_data",
    "LOCAL_ZIP_TEMP": "Z:\\test_zips",
    "LOCAL_ZIP_SPACE": "30GB",
    "IRODS_TARGET_PATH": "/NPEC/home/daale010",
    "METADATA_EXCEL": "test_metadata.xlsx"
}
```


## Upload preperations
- metadata: Foldername, makes status column
- 

Note, when zipping the user must ensure there is enough diskspace in the 'LOCAL_ZIP_TEMP' folder.