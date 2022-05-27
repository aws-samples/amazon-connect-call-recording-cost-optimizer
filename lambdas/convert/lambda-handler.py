import boto3
import json
import os
import subprocess
import logging
from botocore.exceptions import ClientError
from botocore.client import Config
from urllib.parse import unquote_plus
import tempfile

logger = logging.getLogger()
logger.setLevel(logging.INFO)
s3 = boto3.client('s3', config=Config(signature_version='s3v4'))

def lambda_handler(event, context):

    try:
        if event['Records'][0]['body']:
            s3_source_bucket = os.environ['CONNECT_RECORDING_S3_BUCKET']
            s3_source_key = unquote_plus(event['Records'][0]['body'])
            s3_source_key  = s3_source_key.strip('"')
            # accepted values are 'STANDARD' |'REDUCED_REDUNDANCY'|'STANDARD_IA'|'ONEZONE_IA'|'INTELLIGENT_TIERING'|'GLACIER'
            s3_storage_tier= os.environ['S3_STORAGE_TIER']
            # Fetch existing object tags and check if already converted
            tags = s3.get_object_tagging(Bucket=s3_source_bucket, Key=s3_source_key)["TagSet"]
            logger.info("starting convert process")
            logger.info(s3_source_bucket + s3_source_key)
            input_file_path, output_file_path = create_temp_directory()
            s3_source_signed_url = get_audio_file_presigned_url_from_s3(s3_source_bucket, s3_source_key)
            convert_audio_file(s3_source_signed_url,input_file_path, output_file_path)
            upload_audio_file_to_s3(output_file_path,s3_source_bucket, s3_source_key, s3_storage_tier)
            tag_audio_file_in_s3(s3_source_bucket, s3_source_key)
            remove_temp_directory(input_file_path, output_file_path)
            logger.info(f"converted the file: {s3_source_bucket}/{ s3_source_key}")
        return {
            "convert": {
            'file': s3_source_key,
            }
        }
    except ClientError as e:
        logger.error(e)
        return {
            "convert": {
            'statusCode': 500
            }
        }
    
def create_temp_directory( ):
    workdir = tempfile.mkdtemp()
    input_file_path = os.path.join(workdir, 'input.wav')
    output_file_path = os.path.join(workdir, 'outout.wav')
    return input_file_path, output_file_path

def get_audio_file_presigned_url_from_s3(s3_source_bucket, s3_source_key):
    signed_url_timeout = 60
    s3_source_signed_url = s3.generate_presigned_url('get_object',
        Params={'Bucket': s3_source_bucket, 'Key': s3_source_key},
        ExpiresIn=signed_url_timeout)
    return s3_source_signed_url
    
def retrieve_audio_file_from_s3(s3_source_bucket, s3_source_key,input_file_path):
    try:
        s3.Bucket(s3_source_bucket).download_file(s3_source_key, input_file_path)
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            logger.error("The object does not exist.")
        else:
            raise
    return input_file_path
    

    
def convert_audio_file(s3_source_signed_url,input_file_path, output_file_path):
    cmd = ['/opt/bin/ffmpeg' ,  '-y','-i', s3_source_signed_url,'-hide_banner', '-ac','1','-ar', '8000','-c:a','pcm_alaw',input_file_path]
    subprocess.run(cmd,shell=False)
    # Convert again to fix the header file mismatch related to using tmp storage to convert in and out.
    cmd = ['/opt/bin/ffmpeg' , '-y', '-i', input_file_path,'-hide_banner','-ac','1','-ar', '8000','-c:a','pcm_alaw', output_file_path]
    subprocess.run(cmd,shell=False)


def upload_audio_file_to_s3(output_file_path,s3_source_bucket, s3_source_key, s3_storage_tier):
     # upload to s3
    if os.path.exists(output_file_path):
        try:
            s3.upload_file(output_file_path,s3_source_bucket,s3_source_key)
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                logger.error("Unable to uypload the file to S3.")
            else:
                raise    
        # set the storage tier
        copy_source = {
            'Bucket': s3_source_bucket,
            'Key': s3_source_key
        }
        try:
            s3.copy(
                copy_source, s3_source_bucket,s3_source_key,
                ExtraArgs = {
                'StorageClass': s3_storage_tier,
                'MetadataDirective': 'COPY'
                }
            )
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                logger.error("Unable to copy the file to S3.")
            else:
                raise
            
    
def tag_audio_file_in_s3(s3_source_bucket, s3_source_key):
    try:
        s3.put_object_tagging(
            Bucket=s3_source_bucket,
            Key=s3_source_key,    
            Tagging={
                'TagSet': [
                    {
                        'Key': 'convert-batch',
                        'Value': 'true',
                    }
            
                ]
            }
        )
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            logger.error("Unable to copy the file to S3.")
        else:
            raise
    
def remove_temp_directory(input_file_path, output_file_path):
    if os.path.exists(input_file_path):
        os.remove(input_file_path)
     # cleanup tmp directory          
    if os.path.exists(output_file_path):
        os.remove(output_file_path)