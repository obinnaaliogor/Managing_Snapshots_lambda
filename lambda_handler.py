
import boto3
import datetime
import os
import logging
from botocore.exceptions import ClientError
from typing import List, Dict, Any

# Constants
SNS_TOPIC_ARN = os.environ['SNS_TOPIC_ARN']
SNAPSHOT_RETENTION_DAYS = int(os.environ['SNAPSHOT_RETENTION_DAYS'])
WARNING_DAYS = int(os.environ.get('WARNING_DAYS', 5))
DRY_RUN = os.environ.get('DRY_RUN', 'false').lower() == 'true'
BATCH_SIZE = int(os.environ.get('BATCH_SIZE', 100))

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize the EC2 and SNS clients
ec2_client = boto3.client('ec2')
sns_client = boto3.client('sns')
cloudwatch = boto3.client('cloudwatch')

def validate_env_vars() -> None:
    """Validate required environment variables."""
    if not SNS_TOPIC_ARN or not SNAPSHOT_RETENTION_DAYS:
        raise ValueError("Missing required environment variables")
    if SNAPSHOT_RETENTION_DAYS <= 0 or WARNING_DAYS < 0:
        raise ValueError("Invalid retention or warning days")

def get_all_snapshots() -> List[Dict[str, Any]]:
    """Retrieve all snapshots owned by the current account."""
    snapshots = []
    paginator = ec2_client.get_paginator('describe_snapshots')
    for page in paginator.paginate(OwnerIds=['self']):
        snapshots.extend(page['Snapshots'])
    return snapshots

def send_sns_message(message: str) -> None:
    """Send a message to the configured SNS topic."""
    try:
        sns_client.publish(TopicArn=SNS_TOPIC_ARN, Message=message)
    except ClientError as e:
        logger.error(f"Failed to send SNS message: {e}")

def process_snapshot(snapshot: Dict[str, Any], current_date: datetime.datetime) -> None:
    """Process each snapshot based on its age and usage."""
    if snapshot.get('Tags', []) and any(tag['Key'] == 'ExemptFromDeletion' for tag in snapshot['Tags']):
        logger.info(f"Snapshot {snapshot['SnapshotId']} is exempt from deletion")
        return

    snapshot_age = (current_date - snapshot['StartTime']).days
    if snapshot_age > SNAPSHOT_RETENTION_DAYS:
        days_until_deletion = snapshot_age - SNAPSHOT_RETENTION_DAYS
        if days_until_deletion <= WARNING_DAYS:
            if days_until_deletion > 0:
                message = f"Snapshot {snapshot['SnapshotId']} will be deleted in {days_until_deletion} days."
            else:
                message = f"Snapshot {snapshot['SnapshotId']} will be deleted today."
            send_sns_message(message)

        if days_until_deletion <= 0:
            try:
                amis = ec2_client.describe_images(Filters=[{'Name': 'block-device-mapping.snapshot-id', 'Values': [snapshot['SnapshotId']]}])['Images']
                if not amis:
                    if not DRY_RUN:
                        ec2_client.delete_snapshot(SnapshotId=snapshot['SnapshotId'])
                    message = f"{'[DRY RUN] Would have deleted' if DRY_RUN else 'Deleted'} snapshot {snapshot['SnapshotId']} which exceeded retention period."
                    send_sns_message(message)
                    increment_metric('SnapshotsDeleted')
                else:
                    message = f"Snapshot {snapshot['SnapshotId']} is in use by AMI(s) and will not be deleted."
                    send_sns_message(message)
            except ClientError as e:
                logger.error(f"Error processing snapshot {snapshot['SnapshotId']}: {e}")
                increment_metric('SnapshotProcessingErrors')

def process_snapshots_in_batches(snapshots: List[Dict[str, Any]], current_date: datetime.datetime) -> None:
    """Process snapshots in batches to prevent timeout."""
    for i in range(0, len(snapshots), BATCH_SIZE):
        batch = snapshots[i:i+BATCH_SIZE]
        for snapshot in batch:
            process_snapshot(snapshot, current_date)
        logger.info(f"Processed batch of {len(batch)} snapshots")

def increment_metric(metric_name: str) -> None:
    """Increment a custom CloudWatch metric."""
    try:
        cloudwatch.put_metric_data(
            Namespace='SnapshotCleanup',
            MetricData=[
                {
                    'MetricName': metric_name,
                    'Value': 1,
                    'Unit': 'Count'
                },
            ]
        )
    except ClientError as e:
        logger.error(f"Failed to increment metric {metric_name}: {e}")

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Lambda function entry point."""
    try:
        validate_env_vars()
        current_date = datetime.datetime.now(datetime.timezone.utc)
        snapshots = get_all_snapshots()

        process_snapshots_in_batches(snapshots, current_date)

        logger.info(f"Processed {len(snapshots)} snapshots")
        increment_metric('SnapshotsProcessed')
        return {'statusCode': 200, 'body': 'Snapshot cleanup completed successfully!'}
    except ValueError as ve:
        logger.error(f"Validation error: {ve}")
        return {'statusCode': 400, 'body': f"Validation error: {ve}"}
    except ClientError as ce:
        logger.error(f"AWS API error: {ce}")
        increment_metric('AWSAPIErrors')
        return {'statusCode': 500, 'body': 'An error occurred during AWS API call'}
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        increment_metric('UnexpectedErrors')
        return {'statusCode': 500, 'body': 'An unexpected error occurred'}
