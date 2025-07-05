"""Used to import Tiltify donation exports and store in a VictoriaMetrics Database"""

import argparse
import logging
import os
import sys

import pandas as pd
import urllib3

DB_HOSTNAME = os.getenv("DB_HOSTNAME", "localhost")

CAMPAIGN_NAME = os.getenv("CAMPAIGN_NAME", "")
TEAM_NAME = os.getenv("TEAM_NAME", None)

FORMATTING = "1:metric:donation,2:time:unix_ms,3:label:reward,4:label:poll,5:label:target,6:label:campaign"
FORMATTING_TOTAL = "1:metric:donation_total,2:time:unix_ms,3:label:campaign"
FORMATTING_COUNT_TOTAL = "1:metric:donation_count_total,2:time:unix_ms,3:label:campaign"
if TEAM_NAME:
    FORMATTING += ",7:label:team"
    FORMATTING_TOTAL += ",4:label:team"
    FORMATTING_COUNT_TOTAL += ",4:label:team"
DELETE_URL = f"http://{DB_HOSTNAME}:8428/api/v1/admin/tsdb/delete_series"
INGEST_URL = f"http://{DB_HOSTNAME}:8428/api/v1/import/csv?format="
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


class Campaign:
    def __init__(self, name=""):
        self.campaign_name = name
        self.donation_total = 0
        self.donation_count_total = 0
        logging.info("Campaign Name: %s", self.campaign_name)

    def delete_data(self):
        r = http.request(
            "POST",
            DELETE_URL,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            body="match[]=donation",
        )
        logging.debug("Response Code: %s", r.status)
        r = http.request(
            "POST",
            DELETE_URL,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            body="match[]=donation_total",
        )
        logging.debug("Response Code: %s", r.status)

        r = http.request(
            "POST",
            DELETE_URL,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            body="match[]=donation_count_total",
        )
        logging.debug("Response Code: %s", r.status)

    def upload_data(self, metric_str, metric_format):
        r = http.request(
            "POST",
            INGEST_URL + metric_format,
            headers={"Content-Type": "application/json"},
            body=metric_str,
        )
        logging.debug("Response Code: %s", r.status)

    def update_totals(self, row):
        timestamp = row["Time of Donation"]
        timestamp_parsed = parse_timestamp(timestamp)
        self.donation_total += float(row["Donation Amount"])
        self.donation_count_total += 1
        donation_total_clean = round(self.donation_total, 2)
        total_array = [str(donation_total_clean), timestamp_parsed, self.campaign_name]
        total_metric_str = ",".join(total_array)
        count_total_array = [
            str(self.donation_count_total),
            timestamp_parsed,
            self.campaign_name,
        ]
        count_metric_str = ",".join(count_total_array)
        logging.debug("Timestamp %s", timestamp)
        logging.debug("Donation Total: %s", donation_total_clean)
        logging.debug("Donation Count: %s", self.donation_count_total)
        return total_metric_str, count_metric_str


class TiltifyDonation:
    def __init__(self):
        self.donation_data = 0

    def process_entry(self, row, campaign):
        timestamp = row["Time of Donation"]
        timestamp_parsed = parse_timestamp(timestamp)
        data_array = [
            str(row["Donation Amount"]),
            timestamp_parsed,
            str(row["Reward Quantity"]),
            row["Poll Name"],
            row["Target Name"],
            campaign,
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
    if args.clear:
        print("Data will be cleared from database!!!!!")
        answer = input("Continue?")
        if answer.lower() not in ["y", "yes"]:
            print("Skipping")
        else:
            Campaign().delete_data()
    logging.info(f"Processing Files: {dono_files}")
    for file in dono_files:
        logging.info(f"Importing file: {file}")
        campaign_name = CAMPAIGN_NAME
        if not campaign_name:
            campaign_name = os.path.splitext(os.path.basename(file))[0]
            campaign_name = campaign_name.split("tiltify-export-")[1]
            campaign_name = campaign_name.split("-fact-donations")[0]
        campaign = Campaign(campaign_name)

        with open(file, "r", encoding="utf8") as csvfile:
            csv_data = process_csv_vm(csvfile)
        print(f"About to import {len(csv_data)} donations for {campaign_name}")
        answer = input("Continue?")
        if answer.lower() not in ["y", "yes"]:
            print("exiting")
            sys.exit()
        for index, row in csv_data.iterrows():
            logging.debug(index)
            metric_str = dono.process_entry(row, campaign.campaign_name)
            total_metric_str, count_metric_str = campaign.update_totals(row)
            if TEAM_NAME:
                metric_str += f",{TEAM_NAME}"
                total_metric_str += f",{TEAM_NAME}"
                count_metric_str += f",{TEAM_NAME}"
            logging.debug("Uploading donation")
            campaign.upload_data(metric_str, FORMATTING)
            logging.debug("Uploading donation total")
            campaign.upload_data(total_metric_str, FORMATTING_TOTAL)
            campaign.upload_data(count_metric_str, FORMATTING_COUNT_TOTAL)
        logging.info("Donation Total: %s", campaign.donation_total)
        logging.info("Donation Count: %s", campaign.donation_count_total)


if __name__ == "__main__":
    run()
