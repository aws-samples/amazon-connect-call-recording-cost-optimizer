# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import json
import boto3
import os
import logging
from datetime import date, datetime, timedelta
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)
s3 = boto3.client('s3')
    

def recording_file_date_time_prefix_builder(input_date):
        dt_specific_date = input_date
        dt_month = str(dt_specific_date.strftime("%m"))
        dt_year = str(dt_specific_date.year)
        dt_day =  str(dt_specific_date.day)
        
        # 0 pad the month and day
        if len(dt_month) < 2:
            dt_month = '0' + dt_month
        if len(dt_day) < 2:
            dt_day = '0' + dt_day
            
        return dt_year, dt_month, dt_day;


def lambda_handler(event, context):
    
    try:
        
        CONNECT_RECORDING_S3_BUCKET = os.environ['CONNECT_RECORDING_S3_BUCKET']
        MAX_KEYS = int(os.environ['MAX_KEYS'])
        PREFIX = os.environ['PREFIX']
        NUM_DAYS_AGE = int(os.environ['NUM_DAYS_AGE'])
        
        if event["specific_date"]:
            specific_date = event.get('specific_date', None)
            specific_date = datetime.strptime(specific_date,"%m/%d/%Y") 
            dt_year, dt_month, dt_day = recording_file_date_time_prefix_builder(specific_date)
            logger.info(f"Specific date passed {specific_date}")
        else:
            # prefix filter the s3 object to date NUM_DAYS_AGE days ago
            back_date = datetime.now() - timedelta(days=NUM_DAYS_AGE)
            dt_year, dt_month, dt_day = recording_file_date_time_prefix_builder(back_date)
            logger.info(f"{back_date} is the date minus age of {NUM_DAYS_AGE} days.")
            
        FULL_PREFIX = f'{str(PREFIX)}{str(dt_year)}/{str(dt_month)}/{str(dt_day)}/'
        
        logger.info(str(CONNECT_RECORDING_S3_BUCKET + FULL_PREFIX) + " is the new prefix")
        
        if event["NextContinuationToken"]:
            logger.info("Passed in the continuation token from step function. Will use it.")
            continuation_token = event["NextContinuationToken"]
            response = list_audio_files_in_s3(CONNECT_RECORDING_S3_BUCKET, FULL_PREFIX, MAX_KEYS,continuation_token)
        else:
            logger.info("No continuation token passed from step function.")
            response = list_audio_files_in_s3(CONNECT_RECORDING_S3_BUCKET, FULL_PREFIX, MAX_KEYS)
        
        if response["KeyCount"]>0:
            contents = json.loads(json.dumps(response["Contents"], default=str))
            try:
                continuation_token = response["NextContinuationToken"]
                
            except KeyError:
                continuation_token =""
                logger.info("No keys returned error.")
            
        else:
            logger.info("No object keys returned.")
            contents = ""
            continuation_token =""
            
    
        return {
            'files': contents,
            'NextContinuationToken': continuation_token
        }
        
    except ClientError as e:
        logging.error(e)
        return {
            "convert": {
            'statusCode': 500
            }
        }
        
def list_audio_files_in_s3(CONNECT_RECORDING_S3_BUCKET, FULL_PREFIX, MAX_KEYS,continuation_token='' ):
    try:
        if continuation_token:
            response =s3.list_objects_v2(Bucket=CONNECT_RECORDING_S3_BUCKET,Prefix=FULL_PREFIX,MaxKeys=MAX_KEYS,ContinuationToken=continuation_token)
        else:
            response = s3.list_objects_v2(Bucket=CONNECT_RECORDING_S3_BUCKET,Prefix=FULL_PREFIX,MaxKeys=MAX_KEYS)
            logger.info(response)
    except ClientError as e:
        logger.error("Unable to list the bucket")
        logger.error(e)
        raise
    return response