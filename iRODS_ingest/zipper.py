import multiprocessing
import shutil
import logging
import os
from datetime import datetime
from time import sleep
from zipfile import ZipFile, BadZipFile


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
                # Shutil adds zip extension automatically
                zip_root, _ = os.path.splitext(row_dict['_zipPath'])
                shutil.make_archive(zip_root, 'zip', row_dict['_Path'])
                logging.info(f"Zipper {self.id} zipped {row_dict['_Path']} in {datetime.now() - start_time}")
                if self.check_zip(row_dict['_zipPath']):
                    self.zipped_files_queue.put(row_dict)
                else:
                    logging.error(f"Zipper {self.id} failed to zip {row_dict['_Path']}")
                    exit(1)
            except Exception as e:
                logging.error(f"Error zipping file {row_dict['_Path']}: {e}")

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
