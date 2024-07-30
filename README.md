# iRODS ingest
Configurable script to move data to iRODS/tape.
It also adds the metadata provided in the Excel file.
How it works in a nutshell:
- 1.Load excel and check if the Foldernames with a 'v' in the_to_upload
column exists in the folder 
- 2. Generate 'in_progress.csv' this file is used to store the irods, zip paths and the current status of the processing
- 3. Use multiprocessing to zip folders if desired
- 4. Upload the files/folder to iRODS
- 5. Add the metadata to the file/folder
- 6. If desired send the file to tape
- 7. Check if the file is on tape

Statusses in the `_status` columns and the transfer from one to another is vizualized in the image below
<div align="center">
![statusses](/docs/statusses.png)
</div>

# NOTE: finish readme

## Important notes:
- Issue with irods path prevents the entering of `/zone/home/xx`, it does allow `/zone/home/username`. Therefor it's currently diabled and set fixed to `/zone/home`
- It is advised to only upload files, or zipped folders, to tape

NPEC specific details:
- Metadata is set with a 'NPEC_' prefix,in i_operations.py > add_metadata > `tagname = f"NPEC_{col}"`
- iRODS paths are created using the npec structure: `ipath = target_ipath.joinpath('M4', row['System'].upper(), str(row['Year']))` in helpers.py >> create_task_df.

## Preperation
The key in this whole process in the Excel file accopanying the data in the folder.
It has two manditory columns:
- `Foldername`: name of the file/folder to upload, this must be placed in the same folder as the excel file!
- `_to_upload`: Add a `v` in this column to indicate the row should be uploaded, all rows without a `v` are ignored.
Additional columns can be added, these will become metadata fields. They may not start with a `_`!
As example NPEC uses the folowing columns: 
```
- Foldername: name of the file/folder
- Year: 2024
- NPEC module: ClimateCells, Greenhouse, OpenField
- System: G8, Traitseeker, UAVS
- Client: requester of the experiment
- Crop: plants (comma seperated list)
- Comment: free text
- _to_upload: Add a `v`
```


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
    "LOCAL_SOURCE_PATH": "C:\\iRODS_ingest\\test_data", # Folder containing the data to upload
    "LOCAL_ZIP_TEMP": "Z:\\test_zips", # Path to create the temporary zipfiles when uploading folders
    "LOCAL_ZIP_SPACE": "30GB", # Size of the temporary zip area in human readable size, to avoid overflowing disks
    "IRODS_TARGET_PATH": "", # ignored due to bug, see notes
    "METADATA_EXCEL": "test_metadata.xlsx" # Excel file with the list of files to upload and metadata
}
```
