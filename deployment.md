# Line-by-Line Explanation: EC2 Snapshot Cleanup Lambda Function

```python
import boto3
import datetime
import os
import logging
from botocore.exceptions import ClientError
from typing import List, Dict, Any
```
These lines import necessary modules:
- `boto3`: AWS SDK for Python, used to interact with AWS services
- `datetime`: For working with dates and times
- `os`: For accessing environment variables
- `logging`: For logging messages
- `ClientError`: For handling AWS-specific exceptions
- `typing`: For type hinting

```python
SNS_TOPIC_ARN = os.environ['SNS_TOPIC_ARN']
SNAPSHOT_RETENTION_DAYS = int(os.environ['SNAPSHOT_RETENTION_DAYS'])
WARNING_DAYS = int(os.environ.get('WARNING_DAYS', 5))
DRY_RUN = os.environ.get('DRY_RUN', 'false').lower() == 'true'
BATCH_SIZE = int(os.environ.get('BATCH_SIZE', 100))
```
These lines define constants from environment variables:
- `SNS_TOPIC_ARN`: ARN of the SNS topic for notifications
- `SNAPSHOT_RETENTION_DAYS`: Number of days to retain snapshots
- `WARNING_DAYS`: Number of days before deletion to send a warning
- `DRY_RUN`: If true, don't actually delete snapshots
- `BATCH_SIZE`: Number of snapshots to process in each batch

```python
logger = logging.getLogger()
logger.setLevel(logging.INFO)
```
These lines set up logging at the INFO level.

```python
ec2_client = boto3.client('ec2')
sns_client = boto3.client('sns')
cloudwatch = boto3.client('cloudwatch')
```
These lines initialize boto3 clients for EC2, SNS, and CloudWatch services.

```python
def validate_env_vars() -> None:
    if not SNS_TOPIC_ARN or not SNAPSHOT_RETENTION_DAYS:
        raise ValueError("Missing required environment variables")
    if SNAPSHOT_RETENTION_DAYS <= 0 or WARNING_DAYS < 0:
        raise ValueError("Invalid retention or warning days")
```
This function validates the environment variables, raising an error if required variables are missing or invalid.

```python
def get_all_snapshots() -> List[Dict[str, Any]]:
    snapshots = []
    paginator = ec2_client.get_paginator('describe_snapshots')
    for page in paginator.paginate(OwnerIds=['self']):
        snapshots.extend(page['Snapshots'])
    return snapshots
```
This function retrieves all snapshots owned by the current account, using pagination to handle large numbers of snapshots.

```python
def send_sns_message(message: str) -> None:
    try:
        sns_client.publish(TopicArn=SNS_TOPIC_ARN, Message=message)
    except ClientError as e:
        logger.error(f"Failed to send SNS message: {e}")
```
This function sends a message to the configured SNS topic, logging an error if it fails.

```python
def process_snapshot(snapshot: Dict[str, Any], current_date: datetime.datetime) -> None:
    if snapshot.get('Tags', []) and any(tag['Key'] == 'ExemptFromDeletion' for tag in snapshot['Tags']):
        logger.info(f"Snapshot {snapshot['SnapshotId']} is exempt from deletion")
        return
```
This part of the `process_snapshot` function checks if the snapshot is tagged as exempt from deletion.

```python
    snapshot_age = (current_date - snapshot['StartTime']).days
    if snapshot_age > SNAPSHOT_RETENTION_DAYS:
        days_until_deletion = snapshot_age - SNAPSHOT_RETENTION_DAYS
        if days_until_deletion <= WARNING_DAYS:
            if days_until_deletion > 0:
                message = f"Snapshot {snapshot['SnapshotId']} will be deleted in {days_until_deletion} days."
            else:
                message = f"Snapshot {snapshot['SnapshotId']} will be deleted today."
            send_sns_message(message)
```
This part calculates the snapshot's age and sends a warning message if it's approaching or past the retention period.

```python
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
```
This part checks if the snapshot is in use by any AMIs. If not, it deletes the snapshot (unless in dry run mode) and sends a notification. If there's an error, it logs it and increments an error metric.

```python
def process_snapshots_in_batches(snapshots: List[Dict[str, Any]], current_date: datetime.datetime) -> None:
    for i in range(0, len(snapshots), BATCH_SIZE):
        batch = snapshots[i:i+BATCH_SIZE]
        for snapshot in batch:
            process_snapshot(snapshot, current_date)
        logger.info(f"Processed batch of {len(batch)} snapshots")
```
This function processes snapshots in batches to prevent timeout issues with large numbers of snapshots.

```python
def increment_metric(metric_name: str) -> None:
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
```
This function increments a custom CloudWatch metric, used for monitoring the script's operation.

```python
def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
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
```
This is the main Lambda function handler. It orchestrates the entire process:
1. Validates environment variables
2. Gets all snapshots
3. Processes snapshots in batches
4. Logs the result and increments metrics
5. Handles different types of errors, logging them and returning appropriate HTTP status codes