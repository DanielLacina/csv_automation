from imap_tools import MailBox, AND
import time
from dotenv import load_dotenv
import os
from datetime import datetime

load_dotenv()

IMAP_SERVER = "imap.gmail.com"
EMAIL_USER = os.getenv("EMAIL_USERNAME")
EMAIL_PASS = os.getenv("EMAIL_PASSWORD")
FOLDER = "INBOX"
SUBJECT_PREFIX = "[New CSV]"
OUTPUT_DIRECTORY = "inbound_attachments"
POLL_INTERVAL = 60


def poll_server():
    while True:
        with MailBox(IMAP_SERVER).login(EMAIL_USER, EMAIL_PASS, FOLDER) as mb:
            for msg in mb.fetch(AND(subject=f"{SUBJECT_PREFIX}*", seen=False)):
                for att in msg.attachments:
                    file_type = att.filename.split(".")[-1]
                    if file_type == "csv":
                        filename = (
                            f"csv_{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}.csv"
                        )
                        with open(f"{OUTPUT_DIRECTORY}/{filename}", "wb") as f:
                            f.write(att.payload)

        time.sleep(POLL_INTERVAL / 15)


if __name__ == "__main__":
    if not os.path.exists(OUTPUT_DIRECTORY):
        os.makedirs(OUTPUT_DIRECTORY)
    poll_server()
