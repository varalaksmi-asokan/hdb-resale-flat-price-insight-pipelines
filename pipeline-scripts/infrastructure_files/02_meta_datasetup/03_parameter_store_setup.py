
import argparse

import boto3

parser = argparse.ArgumentParser(description="HDB pipeline - Parameter Store setup (plain config only, no secrets)")
parser.add_argument("--region", required=True, help="AWS region")
parser.add_argument("--prefix", default="/hdb-pipeline", help="SSM parameter name prefix (must match config.py's SSM_PARAMETER_PREFIX / HDB_SSM_PREFIX)")
parser.add_argument("--collection-api-base", default="https://api-production.data.gov.sg/v2/public/api")
parser.add_argument("--dataset-api-base", default="https://api-open.data.gov.sg/v1/public/api")
args = parser.parse_args()

ssm = boto3.client("ssm", region_name=args.region)

PARAMETERS = {
    f"{args.prefix}/collection_api_base": args.collection_api_base,
    f"{args.prefix}/dataset_api_base": args.dataset_api_base,
}

print("============================================================")
print("HDB Pipeline - Parameter Store Setup")
print(f"Region : {args.region}")
print(f"Prefix : {args.prefix}")
print("============================================================")

for name, value in PARAMETERS.items():
    ssm.put_parameter(Name=name, Value=value, Type="String", Overwrite=True)
    print(f"  {name} = {value}")

print("Parameter Store setup complete.")
print(
    "Note: the AWS account id and the SNS topic ARN are intentionally NOT "
    "stored here or anywhere else - they're resolved live at runtime. See "
    "common.py's get_account_id() and send_alert()."
)
