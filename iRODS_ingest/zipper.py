import multiprocessing
import shutil
import logging
import os
from datetime import datetime
from time import sleep
from zipfile import ZipFile, BadZipFile
from subprocess import run, CalledProcessError, PIPE


class ZipperProcess(multiprocessing.Process):
    """Process to zip files"""
    def __init__(self, stop_worker: multiprocessing.Event,
                 files_to_zip_queue: multiprocessing.Queue,
                 zipped_files_queue: multiprocessing.Queue,
                 disk_space_lock: multiprocessing.Lock,
                 free_diskspace: multiprocessing.Value,
                 id: int):
        super().__init__()
        self.files_to_zip_queue = files_to_zip_queue
        self.zipped_files_queue = zipped_files_queue
        self.stop_worker = stop_worker
        self.disk_space_lock = disk_space_lock
        self.free_diskspace = free_diskspace
        self.id = id

        # check for winrar
        self.winrar_path = self.get_winrar_path()
        if self.winrar_path:
            logging.info("WinRAR detected")

    def run(self):
        while not self.stop_worker.is_set():
            row_dict = self.files_to_zip_queue.get()
            if 'NONE' in row_dict.keys() or self.stop_worker.is_set():
                # Sentinel value to indicate the end of the queue
                logging.info("Stopping ZipperProcess %d", self.id)
                self.zipped_files_queue.put(self.id)
                break
            try:
                while True:
                    with self.disk_space_lock:
                        if row_dict['_size'] <= self.free_diskspace.value:
                            self.free_diskspace.value -= row_dict['_size']
                            break
                    logging.info("%d Not enough free diskspace, waiting for more", self.id)
                    sleep(300)
                start_time = datetime.now()
                if self.winrar_path:
                    status = self.zip_file_with_winrar(self.winrar_path, row_dict['_Path'], row_dict['_zipPath'])
                else:
                    status = self.zip_file_with_shutil(row_dict['_Path'], row_dict['_zipPath'])
                logging.info(f"Zipper {self.id} zipped {row_dict['_Path']} in {datetime.now() - start_time}")
                if status and self.check_zip(row_dict['_zipPath']):
                    self.zipped_files_queue.put(row_dict)
                else:
                    logging.error(f"Zipper {self.id} failed to zip {row_dict['_Path']}")
                    exit(1)
            except Exception as e:
                logging.error(f"Error zipping file {row_dict['_Path']}: {e}")

    def get_winrar_path(self) -> str:
        """Get the installation path of WinRAR by checking common directories."""
        # windows
        common_paths = [
            r"C:\Program Files\WinRAR\winrar.exe",
            r"C:\Program Files (x86)\WinRAR\winrar.exe"
        ]
        for path in common_paths:
            if os.path.exists(path):
                logging.info(f"WinRAR is installed at: {path}")
                return path

        # Ubuntu
        try:
            result = run(["where", "rar"], stdout=PIPE, stderr=PIPE, text=True)
            if result.returncode == 0:
                winrar_path = result.stdout.strip()
                logging.info(f"WinRAR is installed at: {winrar_path}")
                return winrar_path + os.sep + "winrar"
            else:
                logging.error("WinRAR is not installed or not found in PATH.")
                return ""
        except FileNotFoundError:
            logging.error("The 'where' command is not found.")
        return ""

    def zip_file_with_winrar(self, rar_path, local_path, zip_path) -> bool:
        """Zip a file using WinRAR
        Args:
            rar_path: str
                path to the WinRAR executable
            local_path: str
                path to the file to zip
            zip_path: str
                path to the zip file
        Returns:
            bool: True if succesfull
        """
        try:
            # Construct the WinRAR command, -inul is for no output
            command = [
                rar_path, "a", "-afzip" "-ep1",  "-inul", str(zip_path), str(local_path)
            ]
            # Execute the command
            run(command, check=True)
            logging.info(f"Successfully zipped {local_path} to {zip_path}")
            return True
        except CalledProcessError as e:
            logging.error(f"Failed to zip file {local_path}: {e}")
        return False

    def zip_file_with_shutil(self, local_path: str, zip_path: str) -> bool:
        """Zip a file using shutil
        Args:
            local_path: str
                path to the file to zip
            zip_path: str
                path to the zip file
        Returns:
            bool: True if succesfull
        """
        # Shutil adds zip extension automatically
        zip_root, _ = os.path.splitext(zip_path)
        shutil.make_archive(zip_root, 'zip', local_path)
        return True

    def check_zip(self, zip_path: str) -> bool:
        """Check if the zip file is valid
        Args:
            zip_path: str
                path to the zip file
        Returns:
            bool: True if the zip file is valid
        """
        try:
            with ZipFile(zip_path, 'r') as zip_ref:
                bad_file = zip_ref.testzip()
                if bad_file:
                    return False
        except BadZipFile:
            return False
        return True


# Example usage
if __name__ == "__main__":
    files_to_zip_queue = multiprocessing.Queue()
    zipped_files_queue = multiprocessing.Queue()

    # Add files to the queue
    files_to_zip_queue.put("example1.txt")
    files_to_zip_queue.put("example2.txt")
    files_to_zip_queue.put(None)  # Sentinel value to stop the process

    zipper = ZipperProcess(files_to_zip_queue, zipped_files_queue)
    zipper.start()
    zipper.join()

    # Retrieve zipped files from the queue
    while not zipped_files_queue.empty():
        print(zipped_files_queue.get())
