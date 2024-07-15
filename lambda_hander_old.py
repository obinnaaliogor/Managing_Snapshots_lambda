import boto3
import datetime
import os
import logging

# Initialize the EC2 and SNS clients
ec2_client = boto3.client('ec2')
sns_client = boto3.client('sns')

# Get environment variables
SNS_TOPIC_ARN = os.environ['SNS_TOPIC_ARN']
SNAPSHOT_RETENTION_DAYS = int(os.environ['SNAPSHOT_RETENTION_DAYS'])

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    current_date = datetime.datetime.now(datetime.timezone.utc)
    snapshots = ec2_client.describe_snapshots(OwnerIds=['self'])['Snapshots']

    logger.info(f"Found {len(snapshots)} snapshots.")

    for snapshot in snapshots:
        snapshot_age = (current_date - snapshot['StartTime']).days
        logger.info(f"Checking snapshot {snapshot['SnapshotId']} which is {snapshot_age} days old.")

        if snapshot_age >= SNAPSHOT_RETENTION_DAYS:
            amis = ec2_client.describe_images(Filters=[{'Name': 'block-device-mapping.snapshot-id', 'Values': [snapshot['SnapshotId']]}])['Images']
            if not amis:
                try:
                    message = f"Deleting snapshot {snapshot['SnapshotId']} which is {snapshot_age} days old."
                    logger.info(message)
                    sns_client.publish(TopicArn=SNS_TOPIC_ARN, Message=message)
                    response = ec2_client.delete_snapshot(SnapshotId=snapshot['SnapshotId'])
                    logger.info(f"Delete snapshot response: {response}")
                except Exception as e:
                    logger.error(f"Error deleting snapshot {snapshot['SnapshotId']}: {str(e)}")
            else:
                message = f"Snapshot {snapshot['SnapshotId']} is in use by AMI(s) and will not be deleted."
                logger.info(message)
                sns_client.publish(TopicArn=SNS_TOPIC_ARN, Message=message)
        else:
            logger.info(f"Snapshot {snapshot['SnapshotId']} is not old enough to delete.")

    return {
        'statusCode': 200,
        'body': 'Snapshot cleanup completed successfully!'
    }
