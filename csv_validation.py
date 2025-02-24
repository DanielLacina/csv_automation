import pandas as pd
import os
import time
import logging
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import re
from datetime import datetime

INPUT_DIRECTORY = "inbound_attachments"
OUTPUT_DIRECTORY = "ready_to_upload"
REQUIRED_COLUMNS = ["Name", "Email", "SignupDate"]

logging.basicConfig(
    level=logging.ERROR,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="csv_validation_errors.log",
    filemode="a",
)


def is_valid_email(email):
    regex = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
    return re.match(regex, email) is not None


def is_valid_date(date_str):
    try:
        pd.to_datetime(date_str, format="%Y-%m-%d")
        return True
    except ValueError:
        return False


def process_csv(file_path):
    try:
        df = pd.read_csv(file_path)

        missing_columns = [col for col in REQUIRED_COLUMNS if col not in df.columns]
        if missing_columns:
            logging.error(f"File: {file_path} - Missing columns: {missing_columns}")
            return

        if df.empty:
            logging.error(f"File: {file_path} - The file has no rows.")
            return

        if "Email" in df.columns:
            invalid_emails = df[~df["Email"].apply(is_valid_email)]
            if not invalid_emails.empty:
                logging.error(
                    f"File: {file_path} - Invalid email addresses in rows: {invalid_emails.index.tolist()}"
                )

        if "SignupDate" in df.columns:
            invalid_dates = df[~df["SignupDate"].apply(is_valid_date)]
            if not invalid_dates.empty:
                logging.error(
                    f"File: {file_path} - Invalid SignupDate in rows: {invalid_dates.index.tolist()}"
                )

        if (
            not missing_columns
            and not df.empty
            and invalid_emails.empty
            and invalid_dates.empty
        ):
            filename = f"csv_{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}.csv"
            df.to_csv(f"{OUTPUT_DIRECTORY}/{filename}")
            os.remove(file_path)

    except Exception as e:
        logging.error(f"File: {file_path} - Error processing file: {e}")


class CSVFileHandler(FileSystemEventHandler):
    def on_created(self, event):
        if event.is_directory:
            return
        if event.src_path.endswith(".csv"):
            process_csv(event.src_path)


def watch_directory(directory):
    event_handler = CSVFileHandler()
    observer = Observer()
    observer.schedule(event_handler, path=directory, recursive=False)
    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt():
        observer.stop()
    observer.join()


if __name__ == "__main__":
    if not os.path.exists(OUTPUT_DIRECTORY):
        os.makedirs(OUTPUT_DIRECTORY)
    if not os.path.exists(INPUT_DIRECTORY):
        os.makedirs(INPUT_DIRECTORY)
    for filename in os.listdir(INPUT_DIRECTORY):
        file_type = filename.split(".")[-1]
        if file_type == "csv":
            process_csv(f"{INPUT_DIRECTORY}/{filename}")
    watch_directory(INPUT_DIRECTORY)
