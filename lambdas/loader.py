import boto3
import json
import os
import uuid

# boto3
s3 = boto3.client("s3")
sqs = boto3.client("sqs")

# globals
SQS_SEND_MESSAGE_BATCH_SIZE = os.environ.get('SQS_SEND_MESSAGE_BATCH_SIZE', 10)


def send_sqs_batch(entries, sqs_queue_url):
    try: 
        response = sqs.send_message_batch(
            QueueUrl=sqs_queue_url,
            Entries=entries,
        )
        if len(response.get('Failed', [])):
            print('Some entries failed, please retry.')
    except Exception as e:
        print(e)
        raise Exception('Failed during sqs.send_message_batch API call')


def prepare_sqs_entry(bucket, contents):
    keys = []
    for content in contents:
        keys.append(content.get('Key'))

    entry = {}
    body = {}
    body['bucket'] = bucket
    body['keys'] = keys
    entry['Id'] = str(uuid.uuid1())
    entry['MessageBody'] = json.dumps(body)

    return entry


def handler(event, context):
    print(json.dumps(event))
    sqs_batch_entries = []
    exited_early = False
    num_keys_this_iteration = 0

    bucket = event.get('bucket')
    prefix = event.get('prefix')
    page_size = event.get('page_size', 500)
    sqs_queue_url = event.get('queue_url', os.environ.get('SQS_QUEUE_URL'))
    next_token = event.get('next_token', None)
    num_keys_total = event.get('num_keys_total', 0)

    # first time listing objects, don't call with ContinuationToken
    if not next_token:
        response = s3.list_objects_v2(
            Bucket=bucket,
            Prefix=prefix,
            MaxKeys=page_size,
        )
        contents = response.get('Contents')
        sqs_batch_entries.append(prepare_sqs_entry(bucket, contents))
        num_keys_total += len(contents)
        num_keys_this_iteration += len(contents)
        next_token = response.get('NextContinuationToken')

    while next_token:
        # wait until we're at desired batch size to send to SQS
        if len(sqs_batch_entries) == SQS_SEND_MESSAGE_BATCH_SIZE:
            send_sqs_batch(sqs_batch_entries, sqs_queue_url)
            sqs_batch_entries = []

        response = s3.list_objects_v2(
            Bucket=bucket,
            Prefix=prefix,
            MaxKeys=page_size,
            ContinuationToken=next_token,
        )
        contents = response.get('Contents')
        sqs_batch_entries.append(prepare_sqs_entry(bucket, contents))
        num_keys_total += len(response.get('Contents'))
        num_keys_this_iteration += len(contents)
        next_token = response.get('NextContinuationToken')

        # stop processing if Lambda has less than 15 seconds left
        if context.get_remaining_time_in_millis() < 15000:
            print(f'Running out of time, stopping early. Processed {num_keys_this_iteration} S3 objects this iteration, {num_keys_total} in total.')
            event['next_token'] = next_token
            event['num_keys_total'] = num_keys_total
            exited_early = True
            break

    # send any remaining ones since we're batching to SQS
    if len(sqs_batch_entries) > 0:
        send_sqs_batch(sqs_batch_entries, sqs_queue_url)

    if not exited_early:
        print(f'Finished! Processed {num_keys_total} S3 objects in total.')
        if 'next_token' in event:
            del event['next_token']

    return event
