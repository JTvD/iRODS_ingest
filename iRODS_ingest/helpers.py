import logging
import re
import numpy as np
import pandas as pd
from pathlib import Path
from ibridges import Session
from ibridges.path import IrodsPath

import utils as utils


def get_allowed_chars():
    """Returns the allowed characters in iRODS paths of WUR servers"""
    return 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890!-_.*()/'


def verify_filename(filepath: Path):
    """Verify if the filename contains no parts that are not allowed in iRODS
    Args:
        filename: str
            filename to verify
    Returns:
        bool: True if the filename is valid
    """
    allowed_chars = set(get_allowed_chars())
    return all(c in allowed_chars for c in filepath)


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
    ienv = utils.load_json(env_file)
    isession = Session(irods_env=ienv, password=password)
    # Verification if a connection is made
    isession.server_version

    target_ipath = IrodsPath(isession, config['IRODS_TARGET_PATH'])
    if not target_ipath.collection_exists():
        logging.error('Target path does not exist')
        exit(1)
    return source_path, zip_path, target_ipath, ienv


def create_task_df(to_upload_df: pd.DataFrame, source_path: Path,
                   target_ipath: Path, zip_path: Path, isession: Session):
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
        isession: Session
            iRODS session
    Returns:
        to_upload_df: pd.DataFrame
            Added fields: _Path, _status, _zipPath, _iPath, _size
    """
    to_upload_df['_zipPath'] = ""
    to_upload_df['_size'] = np.nan
    for ind, row in to_upload_df.iterrows():
        local_path = source_path.joinpath(row['Foldername'])
        if row['NPEC module'] == 'ClimateCells':
            ipath = target_ipath.joinpath('M4', row['System'], str(row['Year']))
        elif row['NPEC module'] == 'Greenhouse':
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
                to_upload_df.at[ind, '_iPath'] = ""
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
        else:
            logging.error(f"Path is not a file or folder: {local_path}")
            exit(1)
        # check for invallid irods paths
        if not verify_filename(to_upload_df.at[ind, '_iPath']):
            logging.error(f"Invalid iRODS path at index {ind}: {to_upload_df.at[ind, '_iPath']},\
                           for file: {row['Foldername']}. Only {get_allowed_chars()} are allowed")
            exit(1)
        # Check if column names are valid sql identifiers
        for col in row.keys():
            # Skip upload status columns
            if col[0] == '_':
                continue
            elif not check_sql_string(col):
                logging.error(f"Invalid sql identifier at index {ind}: {col}, for file: {row['Foldername']}")
                exit(1)

        # Check if file already exists, if not add it to the right queue
        irods_path = IrodsPath(isession, to_upload_df.at[ind, '_iPath'])
        if (to_upload_df.at[ind, '_zipPath'] or to_upload_df.at[ind, '_status'] == 'File') and irods_path.dataobject_exists():
            logging.info(f"File already exists: {to_upload_df.at[ind, '_iPath']}")
            to_upload_df.at[ind, '_status'] = 'existing ipath'
        elif to_upload_df.at[ind, '_status'] == 'Folder' and irods_path.collection_exists():
            logging.info(f"Folder already exist: {to_upload_df.at[ind, '_iPath']}")
            to_upload_df.at[ind, '_status'] = 'existing ipath'
    return to_upload_df


def check_sql_string(sql_string: str) -> bool:
    """Check if the sql string is valid
    Args:
        sql_string: str
            sql string to check
    Returns:
        bool: True if the sql string is valid
    """

    # Regular expression for valid identifier
    identifier_regex = re.compile(r'^[A-Za-z_@#][A-Za-z0-9_@$# ]*$')

    # Check if the identifier matches the regex
    if not identifier_regex.match(sql_string):
        return False
    # Check if the identifier is a reserved word
    return check_reserved_sql_words(sql_string)


def check_reserved_sql_words(sql_string: str) -> bool:
    """Check if the sql string is a reserved word
    Args:
        sql_string: str
            sql string to check
    Returns:
        bool: True if the sql string is not a reserved word
    """
    reserved_words = {
        "ADD", "EXTERNAL", "PROCEDURE",
        "ALL", "FETCH", "PUBLIC",
        "ALTER", "FILE", "RAISERROR",
        "AND", "FILLFACTOR", "READ",
        "ANY", "FOR", "READTEXT",
        "AS", "FOREIGN", "RECONFIGURE",
        "ASC", "FREETEXT", "REFERENCES",
        "AUTHORIZATION", "FREETEXTTABLE", "REPLICATION",
        "BACKUP", "FROM", "RESTORE"
        "BEGIN", "FULL", "RESTRICT",
        "BETWEEN", "FUNCTION", "RETURN",
        "BREAK", "GOTO", "REVERT",
        "BROWSE", "GRANT", "REVOKE",
        "BULK", "GROUP", "RIGHT",
        "BY", "HAVING", "ROLLBACK",
        "CASCADE", "HOLDLOCK", "ROWCOUNT",
        "CASE", "IDENTITY", "ROWGUIDCOL",
        "CHECK", "IDENTITY_INSERT", "RULE",
        "CHECKPOINT", "IDENTITYCOL", "SAVE",
        "CLOSE", "IF", "SCHEMA",
        "CLUSTERED", "IN", "SECURITYAUDIT",
        "COALESCE", "INDEX", "SELECT",
        "COLLATE", "INNER", "SEMANTICKEYPHRASETABLE",
        "COLUMN", "INSERT", "SEMANTICSIMILARITYDETAILSTABLE",
        "COMMIT", "INTERSECT", "SEMANTICSIMILARITYTABLE",
        "COMPUTE", "INTO"", ""SESSION_USER",
        "CONSTRAINT", "IS", "SET",
        "CONTAINS", "JOIN", "SETUSER",
        "CONTAINSTABLE", "KEY", "SHUTDOWN",
        "CONTINUE", "KILL", "SOME",
        "CONVERT", "LEFT", "STATISTICS",
        "CREATE", "LIKE", "SYSTEM_USER",
        "CROSS", "LINENO", "TABLE",
        "CURRENT", "LOAD", "TABLESAMPLE",
        "CURRENT_DATE", "MERGE", "TEXTSIZE",
        "CURRENT_TIME", "NATIONAL", "THEN",
        "CURRENT_TIMESTAMP", "NOCHECK", "TO",
        "CURRENT_USER", "NONCLUSTERED", "TOP",
        "CURSOR", "NOT", "TRAN",
        "DATABASE", "NULL", "TRANSACTION",
        "DBCC", "NULLIF", "TRIGGER",
        "DEALLOCATE", "OF", "TRUNCATE",
        "DECLARE", "OFF", "TRY_CONVERT",
        "DEFAULT", "OFFSETS", "TSEQUAL",
        "DELETE", "ON", "UNION",
        "DENY", "OPEN", "UNIQUE",
        "DESC", "OPENDATASOURCE", "UNPIVOT",
        "DISK", "OPENQUERY", "UPDATE",
        "DISTINCT", "OPENROWSET", "UPDATETEXT",
        "DISTRIBUTED", "OPENXML", "USE",
        "DOUBLE", "OPTION", "USER",
        "DROP", "OR", "VALUES",
        "DUMP", "ORDER", "VARYING",
        "ELSE", "OUTER", "VIEW",
        "END", "OVER", "WAITFOR",
        "ERRLVL", "PERCENT", "WHEN",
        "ESCAPE", "PIVOT", "WHERE",
        "EXCEPT", "PLAN", "WHILE",
        "EXEC", "PRECISION", "WITH",
        "EXECUTE", "PRIMARY", "WITHIN GROUP",
        "EXISTS", "PRINT", "WRITETEXT",
        "EXIT", "PROC", "ABSOLUTE"
    }
    # \b is a word boundary, so the pattern will only match words
    pattern = re.compile(r'\b' + re.escape(sql_string.upper()) + r'\b')
    for word in reserved_words:
        if pattern.fullmatch(word):
            return False
    return True
