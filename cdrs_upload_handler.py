import time
import os
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

class FileUploadHandler(FileSystemEventHandler):
    def on_created(self, event):
        if event.is_directory:
            return
        #file_name = os.path.basename(event.src_path)
        #print(f"File {file_name} has been uploaded.")
        # Call your Python script here and pass the file name as an argument
        os.system(f"/mnt/myenv/bin/python3.10 /usr/local/freeswitch/scripts/cdrs/cdrs_posting.py")

if __name__ == "__main__":
    event_handler = FileUploadHandler()
    observer = Observer()
    observer.schedule(event_handler, path="/usr/local/freeswitch/log/json_cdr")
    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

