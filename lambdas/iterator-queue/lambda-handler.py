# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import boto3
import json
import os
import logging
from datetime import date, datetime, timedelta
from botocore.exceptions import ClientError
from botocore.client import Config
from urllib.parse import unquote_plus

logger = logging.getLogger()
logger.setLevel(logging.INFO)
s3 = boto3.client('s3', config=Config(signature_version='s3v4'))
sqs = boto3.client('sqs')

CONVER_BATCH_KEY = 'convert-batch'

def check_converted_tag(object_tags):
    for tag_set in object_tags:
        if tag_set["Key"] == CONVER_BATCH_KEY:
            return True
    return False



def lambda_handler(event, context):
    try:
        CONNECT_RECORDING_CONVERT_QUEUE = os.environ['CONNECT_RECORDING_CONVERT_QUEUE']
        CONNECT_RECORDING_S3_BUCKET = os.environ['CONNECT_RECORDING_S3_BUCKET']
        # Set to True to overwrite the existing file
        # Set to False not to convert existing tagged files
        #Agnel : convert this to env variable
        overwrite_previous_converted = os.environ['OVERWRITE_PREVIOUS_CONVERTED']
        
        key_count = 1
        key_count_skipped_tag = 1
        key_count_skipped_notwav = 1 
        
        for obj in event['iterator']['files']:
            s3_source_key = unquote_plus(obj["Key"])

                # convert wav files only
            if s3_source_key.endswith("wav"):
                tags = s3_get_object_tagging(CONNECT_RECORDING_S3_BUCKET, s3_source_key)
                
                # is not tagged converted already or we want to convert
                if check_converted_tag(tags) is False or overwrite_previous_converted is True:
                    s3_key_prefix,s3_key_object = os.path.split(s3_source_key)
                    convert_key = json.dumps(obj["Key"])
                    # Send message to SQS queue
                    sqs_send_message(CONNECT_RECORDING_CONVERT_QUEUE, convert_key)
                    key_count +=  1
                else:
                    key_count_skipped_tag += 1
            else:
                key_count_skipped_notwav += 1
                
        logger.info(f"key_count is {key_count}")    
        logger.info(f"key_count_skipped_tag is {key_count_skipped_tag}")  
        logger.info(f"key_count_skipped_notwav is {key_count_skipped_notwav}")  
        
    except ClientError as e:
        logging.error(e)
        return {
            "convert": {
            'statusCode': 500
            }
        }
        
def s3_get_object_tagging(CONNECT_RECORDING_S3_BUCKET, s3_source_key):
    try:
        tags = s3.get_object_tagging(Bucket=CONNECT_RECORDING_S3_BUCKET, Key=s3_source_key)["TagSet"]
    except ClientError as e:
        logger.error("Unavle to tag S3 object")
        logger.error(e)
        raise
    return tags        
         
    
def sqs_send_message(CONNECT_RECORDING_CONVERT_QUEUE,convert_key):
    try:
        sqs.send_message(
            QueueUrl=CONNECT_RECORDING_CONVERT_QUEUE,
            MessageBody=(
                convert_key
            )
        )
    except ClientError as e:
        logger.error("Unavle to sent Message through SQS")
        logger.error(e)
        raise