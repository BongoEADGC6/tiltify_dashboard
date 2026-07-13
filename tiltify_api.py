"""Fetch donation data from Tiltify API and export for VictoriaMetrics import"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime

import urllib3

API_BASE = "https://v5api.tiltify.com"
TOKEN_URL = f"{API_BASE}/oauth/token"

logger = logging.getLogger()
log_level = os.getenv("LOG_LEVEL", "INFO")
logger.setLevel(log_level)


class TiltifyClient:
    def __init__(self, client_id, client_secret):
        self.client_id = client_id
        self.client_secret = client_secret
        self.http = urllib3.PoolManager()
        self.token = None
        self.user_id = None

    def authenticate(self):
        resp = self.http.request(
            "POST",
            TOKEN_URL,
            fields={
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "grant_type": "client_credentials",
            },
        )
        if resp.status != 200:
            raise RuntimeError(
                f"Auth failed ({resp.status}): {resp.data.decode()}"
            )
        data = json.loads(resp.data)
        self.token = data["access_token"]
        logger.info("Authenticated successfully, token expires in %ss", data.get("expires_in"))

    def _headers(self):
        return {"Authorization": f"Bearer {self.token}"}

    def _get(self, url, params=None):
        resp = self.http.request("GET", url, headers=self._headers(), fields=params)
        if resp.status != 200:
            raise RuntimeError(f"GET {url} failed ({resp.status}): {resp.data.decode()}")
        return json.loads(resp.data)

    def _paginate(self, url, params=None):
        params = dict(params or {})
        params.setdefault("limit", 100)
        all_data = []
        while True:
            data = self._get(url, params)
            items = data.get("data", [])
            all_data.extend(items)
            meta = data.get("metadata", {})
            after = meta.get("after")
            if not after:
                break
            params["after"] = after
        return all_data

    def get_user_by_slug(self, slug):
        data = self._get(f"{API_BASE}/api/public/users/by/slugs/{slug}")
        user = data.get("data")
        if not user:
            raise RuntimeError(f"User not found for slug: {slug}")
        self.user_id = user.get("id")
        logger.info("Resolved user slug '%s' to ID: %s", slug, self.user_id)
        return user

    def get_user_by_id(self, user_id):
        data = self._get(f"{API_BASE}/api/public/users/{user_id}")
        user = data.get("data")
        if not user:
            raise RuntimeError(f"User not found for ID: {user_id}")
        self.user_id = user.get("id")
        logger.info("Resolved user ID: %s", self.user_id)
        return user

    def list_campaigns(self, user_id=None):
        uid = user_id or self.user_id
        if not uid:
            raise RuntimeError("No user ID set. Provide --user-slug or --user-id.")
        campaigns = self._paginate(
            f"{API_BASE}/api/public/users/{uid}/campaigns"
        )
        logger.info("Found %d campaigns", len(campaigns))
        return campaigns

    def get_campaign_polls(self, campaign_id):
        return self._paginate(
            f"{API_BASE}/api/public/campaigns/{campaign_id}/polls"
        )

    def get_campaign_targets(self, campaign_id):
        return self._paginate(
            f"{API_BASE}/api/public/campaigns/{campaign_id}/targets"
        )

    def get_donations(self, campaign_id, completed_after=None, completed_before=None):
        params = {}
        if completed_after:
            params["completed_after"] = completed_after
        if completed_before:
            params["completed_before"] = completed_before
        return self._paginate(
            f"{API_BASE}/api/public/campaigns/{campaign_id}/donations",
            params,
        )


def sanitize(value):
    value = str(value)
    value = value.replace("\\", "\\\\")
    value = value.replace('"', '\\"')
    value = value.replace(",", "\\,")
    value = value.replace("\n", " ").replace("\r", " ")
    return value.strip()


def format_timestamp(iso_string):
    dt = datetime.fromisoformat(iso_string.replace("Z", "+00:00"))
    return dt.strftime("%Y-%m-%d %H:%M:%S.%fZ")


def build_poll_map(polls):
    return {p["id"]: p["name"] for p in polls}


def build_target_map(targets):
    return {t["id"]: t["name"] for t in targets}


def donation_to_csv_row(donation, poll_map, target_map, event_name):
    amount = donation.get("amount", {}).get("value", "0")
    completed_at = donation.get("completed_at", "")
    timestamp = format_timestamp(completed_at) if completed_at else ""

    reward_qty = 0
    for claim in (donation.get("reward_claims") or []):
        reward_qty += claim.get("quantity", 0)

    poll_name = poll_map.get(donation.get("poll_id", ""), "")
    target_name = target_map.get(donation.get("target_id", ""), "")

    return ",".join([
        str(amount),
        timestamp,
        sanitize(str(reward_qty)),
        sanitize(poll_name),
        sanitize(target_name),
        sanitize(event_name),
    ])


def format_csv_header():
    return "Donation Amount,Time of Donation,Reward Quantity,Poll Name,Target Name,Event Name"


def main():
    parser = argparse.ArgumentParser(
        description="Fetch Tiltify donations via API and output VictoriaMetrics CSV"
    )
    parser.add_argument("--client-id", help="Tiltify API client ID (default: $TILTIFY_CLIENT_ID)")
    parser.add_argument("--client-secret", help="Tiltify API client secret (default: $TILTIFY_CLIENT_SECRET)")
    parser.add_argument("--user-slug", help="Your Tiltify user slug (e.g. 'username')")
    parser.add_argument("--user-id", help="Your Tiltify user UUID")
    parser.add_argument("--event-name", default=os.getenv("EVENT_NAME", ""), help="Event name label for VM")
    parser.add_argument("--campaign-id", help="Specific campaign ID (omit to list all)")
    parser.add_argument("--list-campaigns", action="store_true", help="List campaigns and exit")
    parser.add_argument("--completed-after", help="Only donations completed after ISO8601 timestamp")
    parser.add_argument("--completed-before", help="Only donations completed before ISO8601 timestamp")
    parser.add_argument("--output", help="Output file (default: stdout)")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    if args.verbose:
        logger.setLevel(logging.DEBUG)

    client_id = args.client_id or os.getenv("TILTIFY_CLIENT_ID")
    client_secret = args.client_secret or os.getenv("TILTIFY_CLIENT_SECRET")

    if not client_id or not client_secret:
        print("ERROR: TILTIFY_CLIENT_ID and TILTIFY_CLIENT_SECRET must be set", file=sys.stderr)
        sys.exit(1)

    client = TiltifyClient(client_id, client_secret)
    client.authenticate()

    if args.user_slug:
        client.get_user_by_slug(args.user_slug)
    elif args.user_id:
        client.get_user_by_id(args.user_id)

    if args.list_campaigns:
        if not client.user_id:
            print("ERROR: --user-slug or --user-id required to list campaigns", file=sys.stderr)
            sys.exit(1)
        campaigns = client.list_campaigns()
        print(f"{'ID':<40} {'Name':<50} {'Status':<12} {'Raised':<12}")
        print("-" * 114)
        for c in campaigns:
            raised = c.get("total_amount_raised", {}).get("value", "0")
            print(f"{c['id']:<40} {c['name']:<50} {c.get('status', ''):<12} ${raised:<10}")
        return

    if args.campaign_id:
        campaign_ids = [args.campaign_id]
        campaign_names = {args.campaign_id: args.event_name or args.campaign_id}
    else:
        campaigns = client.list_campaigns()
        campaign_ids = [c["id"] for c in campaigns]
        campaign_names = {c["id"]: c["name"] for c in campaigns}

    out_fh = open(args.output, "w", encoding="utf-8") if args.output else sys.stdout

    print(format_csv_header(), file=out_fh)

    total_donations = 0
    for cid in campaign_ids:
        event_name = campaign_names[cid] if not args.event_name else args.event_name
        logger.info("Fetching polls and targets for campaign %s...", cid)
        polls = client.get_campaign_polls(cid)
        targets = client.get_campaign_targets(cid)
        poll_map = build_poll_map(polls)
        target_map = build_target_map(targets)

        logger.info("Fetching donations for campaign %s (%s)...", cid, event_name)
        donations = client.get_donations(
            cid,
            completed_after=args.completed_after,
            completed_before=args.completed_before,
        )
        logger.info("  Found %d donations", len(donations))

        for d in donations:
            row = donation_to_csv_row(d, poll_map, target_map, event_name)
            print(row, file=out_fh)
            total_donations += 1

    if args.output:
        out_fh.close()

    logger.info("Exported %d total donations", total_donations)
    print(f"\nExported {total_donations} donations", file=sys.stderr)


if __name__ == "__main__":
    main()
