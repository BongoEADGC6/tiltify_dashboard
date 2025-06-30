"""Used to import Tiltify donation exports and store in a VictoriaMetrics Database"""

import argparse
import logging
import os
import sys

import pandas as pd
import urllib3

DB_HOSTNAME = os.getenv("DB_HOSTNAME", "localhost")

FORMATTING = "1:metric:donation,2:time:unix_ms,3:label:reward,4:label:poll,5:label:target,6:label:event"
FORMATTING_TOTAL = "1:metric:donation_total,2:time:unix_ms,3:label:event"
FORMATTING_COUNT_TOTAL = "1:metric:donation_count_total,2:time:unix_ms,3:label:event"
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


class Event:
    def __init__(self, name):
        self.event_name = name
        self.donation_total = 0
        self.donation_count_total = 0
        logging.info("Event Name: %s", self.event_name)

    def delete_data(self):
        r = http.request(
            "POST",
            delete_url,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            body="match[]=donation",
        )
        logging.debug("Response Code: %s", r.status)
        r = http.request(
            "POST",
            delete_url,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            body="match[]=donation_total",
        )
        logging.debug("Response Code: %s", r.status)

        r = http.request(
            "POST",
            delete_url,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            body="match[]=donation_count_total",
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

    def update_totals(self, row):
        timestamp = row["Time of Donation"]
        timestamp_parsed = parse_timestamp(timestamp)
        self.donation_total += float(row[1])
        self.donation_count_total += 1
        donation_total_clean = round(self.donation_total, 2)
        total_array = [str(donation_total_clean), timestamp_parsed, self.event_name]
        total_metric_str = ",".join(total_array)
        count_total_array = [
            str(self.donation_count_total),
            timestamp_parsed,
            self.event_name,
        ]
        count_metric_str = ",".join(count_total_array)
        logging.debug("Timestamp %s", timestamp)
        logging.debug("Donation Total: %s", donation_total_clean)
        logging.debug("Donation Count: %s", self.donation_count_total)
        return total_metric_str, count_metric_str


class TiltifyDonation:
    def __init__(self):
        self.donation_data = 0

    def process_entry(self, row, event):
        timestamp = row["Time of Donation"]
        timestamp_parsed = parse_timestamp(timestamp)
        data_array = [
            str(row["Donation Amount"]),
            timestamp_parsed,
            str(row["Reward Quantity"]),
            row["Poll Name"],
            row["Target Name"],
            event,
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
    parser.add_argument("-c", "--clear", action="store_true")
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument("filenames", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    return args


def run():
    dono = TiltifyDonation()
    args = get_args()
    dono_files = args.filenames
    logging.info(f"Processing Files: {dono_files}")
    print(f"About to import from {args.filenames}")
    answer = input("Continue?")
    if answer.lower() not in ["y", "yes"]:
        print("exiting")
        sys.exit()
    for file in dono_files:
        logging.info(f"Importing file: {file}")
        EVENT_NAME = os.getenv("EVENT_NAME", "")
        if not EVENT_NAME:
            EVENT_NAME = os.path.splitext(os.path.basename(file))[0]
            EVENT_NAME = EVENT_NAME.split("tiltify-export-")[1]
            EVENT_NAME = EVENT_NAME.split("-fact-donations")[0]
        event = Event(EVENT_NAME)
        if args.clear:
            print("Data will be cleared from database!!!!!")
            answer = input("Continue?")
            if answer.lower() not in ["y", "yes"]:
                print("Skipping")
            else:
                event.delete_data()
        with open(file, "r", encoding="utf8") as csvfile:
            csv_data = process_csv_vm(csvfile)
        for index, row in csv_data.iterrows():
            logging.debug(index)
            metric_str = dono.process_entry(row, event.event_name)
            total_metric_str, count_metric_str = event.update_totals(row)
            logging.debug("Uploading donation")
            event.upload_data(metric_str, FORMATTING)
            logging.debug("Uploading donation total")
            event.upload_data(total_metric_str, FORMATTING_TOTAL)
            event.upload_data(count_metric_str, FORMATTING_COUNT_TOTAL)
        logging.info("Donation Total: %s", event.donation_total)
        logging.info("Donation Count: %s", event.donation_count_total)


if __name__ == "__main__":
    run()
