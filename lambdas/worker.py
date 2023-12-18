import boto3
import json
import os

# boto3
s3 = boto3.client('s3')
s3r = boto3.resource('s3')

# globals
DELETE_ORIGINAL_FILES = os.environ.get('DELETE_ORIGINAL_FILES', False)


def handler(event, context):
    failed_keys = []

    for record in event.get('Records', []):
        try:
            body = json.loads(record.get('body'))
            bucket = body.get('bucket')
            keys = body.get('keys')
            bucket_object = s3r.Bucket(bucket)

            for key in keys:
                try:
                    print(f'Received S3 object: s3://{bucket}/{key}')

                    # Do something with the S3 object data. Examples:
                    # - send to Firehose
                    # - write to DynamoDB
                    # - write to Aurora
                    # - transform and write back to S3
                    # - call an external API

                    # RECOMMENED: copy file to a new prefix 
                    try:
                        copy_source = {'Bucket': bucket, 'Key': key}
                        first_prefix = key.split('/')[0]
                        new_obj_name = key.replace(first_prefix, f'{first_prefix}_processed')
                        bucket_object.copy(copy_source, new_obj_name)
                        print(f'Copied file to processed location: s3://{bucket}/{new_obj_name}')
                    except Exception as e:
                        print(f'Failed copying file to processed S3 location ({key}): {str(e)}')
                        failed_keys.append(key)
                        continue

                    # RECOMMENDED: delete original file after it's moved to a new prefix
                    # TODO: switch Lambda environment variable to True (defaults to False for safety)
                    if DELETE_ORIGINAL_FILES:
                        try:
                            s3r.Object(bucket, key).delete()
                            print('Deleted file from original S3 location')
                        except Exception as e:
                            print(f'Failed deleting file from original S3 location ({key}): {str(e)}')
                            failed_keys.append(key)
                            continue

                except Exception as e:
                    print(f'Failed processing S3 object: {str(e)}')
                    failed_keys.append(key)

        except Exception as e:
            print(str(e))
            raise Exception('Failed loading body from record')
        
    if len(failed_keys):
        print(f'Finished with {len(failed_keys)} failed keys')

    return True
