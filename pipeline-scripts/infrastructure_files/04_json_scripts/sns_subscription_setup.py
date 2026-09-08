
import argparse
import sys
from pathlib import Path

import boto3

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "05_ETL"))
from config import ALERT_RECIPIENT_EMAILS, SNS_TOPIC_NAME

parser = argparse.ArgumentParser(description="HDB pipeline - subscribe email(s) to the SNS alert topic")
parser.add_argument("--region", required=True, help="AWS region")
parser.add_argument(
    "--email", action="append", default=None,
    help="Email address to subscribe. Repeat for multiple. Defaults to config.ALERT_RECIPIENT_EMAILS "
         "(HDB_ALERT_RECIPIENT_EMAILS env var) if omitted.",
)
parser.add_argument("--topic-name", default=SNS_TOPIC_NAME, help="SNS topic name (must match what setup.sh created)")
args = parser.parse_args()

emails = args.email or ALERT_RECIPIENT_EMAILS
if not emails:
    print(
        "No email address given - pass --email you@example.com (repeatable) "
        "or set HDB_ALERT_RECIPIENT_EMAILS=you@example.com,teammate@example.com first."
    )
    sys.exit(1)

sts = boto3.client("sts", region_name=args.region)
account_id = sts.get_caller_identity()["Account"]
topic_arn = f"arn:aws:sns:{args.region}:{account_id}:{args.topic_name}"

sns = boto3.client("sns", region_name=args.region)

print("============================================================")
print("HDB Pipeline - SNS Email Subscription Setup")
print(f"Topic: {topic_arn}")
print("============================================================")

existing = sns.list_subscriptions_by_topic(TopicArn=topic_arn).get("Subscriptions", [])
already_subscribed = {s["Endpoint"] for s in existing if s["Protocol"] == "email"}

for email in emails:
    if email in already_subscribed:
        print(f"  {email} - already subscribed, skipping")
        continue
    sns.subscribe(TopicArn=topic_arn, Protocol="email", Endpoint=email)
    print(f"  {email} - subscription requested (check inbox for a confirmation email)")

print("------------------------------------------------------------")
print("IMPORTANT: each address above must click the confirmation link AWS")
print("just emailed it - until then it stays PendingConfirmation and will")
print("NOT receive pipeline alerts. Re-run with the same args any time to")
print("check status; already-confirmed addresses print 'already subscribed'.")
