"""Used to import Tiltify donation exports and store in a VictoriaMetrics Database"""

import argparse
import logging
import os
import sys

import pandas as pd
import urllib3

EVENT_NAME = os.getenv("EVENT_NAME", "Donation Event")
DB_HOSTNAME = os.getenv("DB_HOSTNAME", "localhost")

FORMATTING = "1:metric:donation,2:time:unix_ms,3:label:reward,4:label:poll,5:label:target,6:label:event"
delete_url = f"http://{DB_HOSTNAME}:8428/api/v1/admin/tsdb/delete_series"
ingest_url = f"http://{DB_HOSTNAME}:8428/api/v1/import/csv?format="
http = urllib3.PoolManager()

logger = logging.getLogger()
log_level = os.getenv("LOG_LEVEL", "INFO")
logger.setLevel(log_level)


def process_csv_vm(csvfile) -> list:
    data = pd.read_csv(csvfile)
    data["Time of Donation"] = pd.to_datetime(
        data["Time of Donation"], format="%Y-%m-%d %H:%M:%S.%fZ"
    )
    data.sort_values(by="Time of Donation", ascending=True, inplace=True)
    data = data.fillna("")
    return data


def parse_timestamp(timestamp) -> str:
    timestamp_unix_ms = timestamp.timestamp() * 1000
    return str(timestamp_unix_ms).split(".", maxsplit=1)[0]


def sanitize(value) -> str:
    value = str(value)
    value = value.replace("\\", "\\\\")
    value = value.replace('"', '\\"')
    value = value.replace(",", "\\,")
    value = value.replace("\n", " ").replace("\r", " ")
    return value.strip()


class Event:
    def __init__(self, name):
        self.event_name = name

    def delete_data(self):
        r = http.request(
            "POST",
            delete_url,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            body=f'match[]={{__name__="donation",event="{self.event_name}"}}',
        )
        logging.debug("Response Code: %s", r.status)

    def upload_data(self, metric_str, metric_format):
        r = http.request(
            "POST",
            ingest_url + metric_format,
            headers={"Content-Type": "application/json"},
            body=metric_str,
        )
        logging.debug("Response Code: %s", r.status)

class TiltifyDonation:
    def __init__(self):
        self.donation_data = 0

    def process_entry(self, row, event):
        timestamp = row["Time of Donation"]
        timestamp_parsed = parse_timestamp(timestamp)
        data_array = [
            str(row["Donation Amount"]),
            timestamp_parsed,
            sanitize(row["Reward Quantity"]),
            sanitize(row["Poll Name"]),
            sanitize(row["Target Name"]),
            sanitize(event),
        ]
        logging.debug(data_array)
        metric_str = ",".join(data_array)
        return metric_str


def get_args():
    parser = argparse.ArgumentParser(
        prog="ProgramName",
        description="What the program does",
        epilog="Text at the bottom of help",
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument("filenames", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    return args


def run():
    dono = TiltifyDonation()
    event = Event(EVENT_NAME)
    args = get_args()
    dono_files = args.filenames
    logging.info(f"Processing Files: {dono_files}")
    df_list = []
    for file in dono_files:
        logging.info(f"Importing file: {file}")
        with open(file, "r", encoding="utf8") as csvfile:
            df_list.append(process_csv_vm(csvfile))
    csv_data = pd.concat(df_list)
    print(f"About to import {len(csv_data)} entries from {args.filenames}")
    answer = input("Continue?")
    if answer.lower() not in ["y", "yes"]:
        print("exiting")
        sys.exit()
    print("Clearing existing data for this event...")
    event.delete_data()
    for index, row in csv_data.iterrows():
        logging.debug(index)
        metric_str = dono.process_entry(row, event.event_name)
        logging.debug("Uploading donation")
        event.upload_data(metric_str, FORMATTING)
    logging.info("Import complete")


if __name__ == "__main__":
    run()
