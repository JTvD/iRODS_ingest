import subprocess
import logging
import string
from pathlib import Path


class SMB():
    """Simple class to help mount the SMB share if needed"""
    def __init__(self, config: dict):
        """Init uses the smb parameters
        Args:
            dictionary containing:
                SMB_PATH: str
                    path to the server
                SMB_USER: str
                SMB_LETTER: str
                    drive under which the smb path is mounted
        Return:
            -
        """
        self.smb_path = config['SMB_PATH']
        self.username = config['SMB_USER']
        self.drive_letter = config['SMB_LETTER']
        if self.drive_letter[-1] != ':':
            self.drive_letter += ':'

        # Find used drive letters
        mounted_drives = [x + ':' for x in string.ascii_uppercase if Path(x + ":").exists()]
        if self.drive_letter in mounted_drives and not self.is_share_mounted():
            logging.error(f"Drive {self.drive_letter} is already used for something else")
            exit(1)

    def is_share_mounted(self):
        """"Helper to check if the SMB drive is already mounted"""
        result = subprocess.run("net use", shell=True, check=True, capture_output=True, text=True)
        return (self.drive_letter and self.smb_path.rstrip('\\')) in result.stdout

    def mount_share(self, password: str):
        """"Moun the SMB share if not yet mounted
        Args:
            dfs_password: str
                password for the SMB share
        Return:
            -
        """
        if self.is_share_mounted():
            logging.info(f"The drive {self.drive_letter} is already mounted")
            return True
        try:
            subprocess.run(f"net use \"{self.drive_letter}\" {self.smb_path} {password} /user:wur\\{self.username}",
                           shell=True, check=True, capture_output=True, text=True)
            logging.info(f"Mounted {self.drive_letter}")
            return True
        except Exception as e:
            logging.info(f"An error occured while trying to mount {self.drive_letter}: {e}")
            return False


if __name__ == "__main__":
    from getpass import getpass
    import utils
    utils.setup_logger()

    """Simple test script"""
    cfd = Path(__file__).parent
    config = utils.load_json(cfd / "config.json")
    password = getpass(f"Password for {config['SMB']['SMB_USER']}:")
    smb = SMB(config['SMB'])
    smb.mount_share(password)
