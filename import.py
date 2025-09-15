"""Used to import Tiltify donation exports and store in a VictoriaMetrics Database"""

import argparse
import logging
import os
import sys

import pandas as pd
import urllib3


CAMPAIGN_NAME = os.getenv("CAMPAIGN_NAME", "")
TEAM_NAME = os.getenv("TEAM_NAME", "Default Team")

FORMATTING = "1:metric:donation,2:time:unix_ms,3:label:reward,4:label:poll,5:label:target,6:label:campaign,7:label:team"
FORMATTING_TOTAL_BASE = "1:metric:donation_total,2:time:unix_ms"
FORMATTING_COUNT_BASE = "1:metric:donation_count_total,2:time:unix_ms"

DB_HOSTNAME = os.getenv("DB_HOSTNAME", "localhost")
DELETE_URL = f"http://{DB_HOSTNAME}:8428/api/v1/admin/tsdb/delete_series"
INGEST_URL = f"http://{DB_HOSTNAME}:8428/api/v1/import/csv?format="

http = urllib3.PoolManager()

logger = logging.getLogger()
log_level = os.getenv("LOG_LEVEL", "INFO")
logger.setLevel(log_level)


def process_csv_vm(csvfile) -> list:
    """Process a CSV file containing Tiltify donation data.

    Args:
        csvfile: File object containing the CSV data to process

    Returns:
        list: Pandas DataFrame with processed donation data, sorted by timestamp
    """
    data = pd.read_csv(csvfile)
    data["Time of Donation"] = pd.to_datetime(
        data["Time of Donation"], format="%Y-%m-%d %H:%M:%S.%fZ"
    )
    data.sort_values(by="Time of Donation", ascending=True, inplace=True)
    data = data.fillna("")
    return data


def parse_timestamp(timestamp) -> str:
    """Convert a datetime timestamp to Unix milliseconds.

    Args:
        timestamp: datetime object to convert

    Returns:
        str: Unix timestamp in milliseconds as a string
    """
    timestamp_unix_ms = timestamp.timestamp() * 1000
    return str(timestamp_unix_ms).split(".", maxsplit=1)[0]


class TiltifyDonation:
    """Class representing a single Tiltify donation."""

    def __init__(self):
        """Initialize a new TiltifyDonation instance."""
        self.donation_data = 0

    def process_entry(self, row):
        """Process a donation entry by converting its timestamp.

        Args:
            row: Dictionary containing donation data with timestamp

        Returns:
            dict: Processed donation data with Unix timestamp
        """
        row["Time of Donation"] = parse_timestamp(row["Time of Donation"])
        return row


class Team:
    """Class representing a team that can have multiple campaigns.

    Attributes:
        team_name (str): Name of the team
        donation_total (float): Running total of donations
        donation_count_total (int): Count of total donations
        campaigns (dict): Dictionary of Campaign objects
    """

    def __init__(self, name=""):
        """Initialize a new Team instance.

        Args:
            name (str, optional): Name of the team. Defaults to empty string.
        """
        self.team_name = name
        self.donation_total = 0
        self.donation_count_total = 0
        logging.info("Team Name: %s", self.team_name)
        self.campaigns = {}

    def delete_data(self):
        r = http.request(
            "POST",
            DELETE_URL,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            body="match[]=donation{team=" + self.team_name + "}",
        )
        logging.debug("Response Code: %s", r.status)
        r = http.request(
            "POST",
            DELETE_URL,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            body="match[]=donation_total{team=" + self.team_name + "}",
        )
        logging.debug("Response Code: %s", r.status)

        r = http.request(
            "POST",
            DELETE_URL,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            body="match[]=donation_count_total{team=" + self.team_name + "}",
        )
        logging.debug("Response Code: %s", r.status)

    def add_campaign(self, campaign_name):
        """Add a new campaign to the team if it doesn't exist.

        Args:
            campaign_name (str): Name of the campaign to add

        Returns:
            Campaign: The new or existing campaign object
        """
        if campaign_name not in self.campaigns:
            self.campaigns[campaign_name] = Campaign(
                name=campaign_name, team_name=self.team_name
            )
            logging.info("Added Campaign: %s", campaign_name)
        else:
            logging.info("Campaign already exists: %s", campaign_name)
        return self.campaigns[campaign_name]


class Campaign:
    """Class representing a fundraising campaign.

    Attributes:
        campaign_name (str): Name of the campaign
        team_name (str): Name of the team this campaign belongs to
        donation_total (float): Total donations for this campaign
        donation_count_total (int): Total number of donations
    """

    def __init__(self, name, team_name):
        """Initialize a new Campaign instance.

        Args:
            name (str): Name of the campaign
            team_name (str): Name of the team this campaign belongs to
        """
        self.campaign_name = name
        self.team_name = team_name
        self.donation_total = 0
        self.donation_count_total = 0
        logging.info("Campaign Name: %s", self.campaign_name)

    def delete_data(self):
        r = http.request(
            "POST",
            DELETE_URL,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            body="match[]=donation{campaign=" + self.campaign_name + "}",
        )
        logging.debug("Response Code: %s", r.status)
        r = http.request(
            "POST",
            DELETE_URL,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            body="match[]=donation_total{campaign=" + self.campaign_name + "}",
        )
        logging.debug("Response Code: %s", r.status)

        r = http.request(
            "POST",
            DELETE_URL,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            body="match[]=donation_count_total{campaign=" + self.campaign_name + "}",
        )
        logging.debug("Response Code: %s", r.status)

    def upload_data(self, metric_str, metric_format):
        """Upload metric data to the VictoriaMetrics database.

        Args:
            metric_str (str): Comma-separated metric data string
            metric_format (str): Format string for the metric data
        """
        r = http.request(
            "POST",
            INGEST_URL + metric_format,
            headers={"Content-Type": "application/json"},
            body=metric_str,
        )
        logging.debug("Response Code: %s", r.status)

    def add_donation(self, row):
        """Add a new donation to the campaign and update totals.

        Args:
            row: DataFrame row containing the donation data
        """
        dono = TiltifyDonation()
        dono_data = dono.process_entry(row)
        self._upload_donation(dono_data)
        dono_data.append(self.campaign_name)
        dono_data.append(self.team_name)
        logging.debug("Uploading donation")
        self._update_campaign_total(dono_data)
        self._update_team_total(dono_data)

    def _upload_donation(self, dono):
        data_array = [
            str(dono["Donation Amount"]),
            dono["Time of Donation"],
            str(dono["Reward Quantity"]),
            dono["Poll Name"],
            dono["Target Name"],
            self.campaign_name,
            self.team_name,
        ]
        logging.debug(data_array)
        upload_str = ",".join(data_array)
        self.upload_data(upload_str, FORMATTING)
        return data_array

    def _update_campaign_total(self, dono):
        timestamp = dono["Time of Donation"]
        timestamp_parsed = parse_timestamp(timestamp)
        self.donation_total += float(dono["Donation Amount"])
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
        logging.debug("Uploading campaign total")
        extra_labels = ",3:label:campaign"
        FORMATTING_TOTAL = FORMATTING_TOTAL_BASE + extra_labels
        FORMATTING_COUNT_TOTAL = FORMATTING_COUNT_BASE + extra_labels
        self.upload_data(total_metric_str, FORMATTING_TOTAL)
        self.upload_data(count_metric_str, FORMATTING_COUNT_TOTAL)

    def _update_team_total(self, row):
        self.donation_total += float(row["Donation Amount"])
        self.donation_count_total += 1
        donation_total_clean = round(self.donation_total, 2)
        total_array = [
            str(donation_total_clean),
            row["Time of Donation"],
            self.team_name,
        ]
        total_metric_str = ",".join(total_array)
        count_total_array = [
            str(self.donation_count_total),
            row["Time of Donation"],
            self.team_name,
        ]
        count_metric_str = ",".join(count_total_array)
        extra_labels = ",3:label:team"
        FORMATTING_TOTAL = FORMATTING_TOTAL_BASE + extra_labels
        FORMATTING_COUNT_TOTAL = FORMATTING_COUNT_BASE + extra_labels
        self.upload_data(total_metric_str, FORMATTING_TOTAL)
        self.upload_data(count_metric_str, FORMATTING_COUNT_TOTAL)


def get_args():
    """Parse and return command line arguments.

    Returns:
        argparse.Namespace: Parsed command line arguments
    """
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
    """Main function to process Tiltify donation files and upload to database.

    Reads donation CSV files, processes them, and uploads the data to VictoriaMetrics.
    Allows user confirmation before processing each file.
    """
    args = get_args()
    dono_files = args.filenames
    team = Team(TEAM_NAME)

    logging.info("Processing Files: %s" % dono_files)
    for file in dono_files:
        logging.info("Importing file: %s" % file)
        campaign_name = CAMPAIGN_NAME
        if not campaign_name:
            campaign_name = os.path.splitext(os.path.basename(file))[0]
            campaign_name = campaign_name.split("tiltify-export-")[1]
            campaign_name = campaign_name.split("-fact-donations")[0]
        campaign = team.add_campaign(campaign_name)

        with open(file, "r", encoding="utf8") as csvfile:
            csv_data = process_csv_vm(csvfile)
        print(f"About to import {len(csv_data)} donations for {campaign_name}")
        answer = input("Continue?")
        if answer.lower() not in ["y", "yes"]:
            print("exiting")
            sys.exit()
        for index, row in csv_data.iterrows():
            logging.debug(index)
            campaign.add_donation(row)


if __name__ == "__main__":
    run()
