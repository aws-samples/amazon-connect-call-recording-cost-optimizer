# Amazon Connect Call Recording Cost Optimizer

## Overview

This solution enables you to reduce the storage cost of archived contact center call recordings, by automating scheduling, storage tiering, and resampling of contact center call recording files. The solution is designed as serverless asynchronous workflow, utilizing AWS Step Functions, Amazon SQS, and AWS Lambda. 


![Alt text](call-recordining-convert-arch-2.png?raw=true "Call Recording Conversion Solution")


# Solution
A daily Amazon EventBridge scheduled rule triggers the AWS Step Functions workflow, which orchestrates the batch resampling process for all the files that are older than 7 days. For instance, if today was 15th of Feb, the workflow would not resample files between 15th of February and 8th of February, and it would only process files that are older than 8th of February.

In the First task ("7 days ago recordings s3 iterator"), a Lambda function ("step-iterator") iterates through Amazon Connect call recording S3 bucket using the ListObjectsV2 API obtaining the call recordings (1000 objects per iteration) with the S3 date prefix from 7 days ago.

The next step ("Add files to convert Queue") invokes a Lambda function("stepfunction-queue") that sends a message, into the SQS queue("connect_audio_convert"), for each Amazon Connect call recording file retrieved from S3. A resampling Lambda ("media-convert-files") receives SQS messages, via event source mapping Lambda integration. Each concurrent Lambda("connect_audio_convert") invocation downloads an Amazon Connect call recording file from S3, resamples the file using ffmpeg, and adds "converted" S3 object metadata. Finally, Lambda function ("media-convert-files") uploads the resampled call recording file to S3, overwriting the original S3 Standard Storage Class call recording file, setting the new cost optimized Glacier Instant Retrieval Storage Class. 

# Step Function workflow diagram

AWS Step Functions workflow handles failures, through logging, and a dead-letter queue (DLQ), ("connect_audio_convert_dlq"), to collect messages that can't be processed successfully. The resampling Lambda function ("media-convert-files") uses another DLQ ("connect_audio_convert_dest_failure_queue") for files that can't be resampled. A Step Function task ("More files to process?") monitors the SQS queue ("connect_audio_convert"), using the Step Functions AWS SDK integration with SQS, and completes the Step Function workflow when the queue ("connect_audio_convert") is emptied.


![Alt text](call-recordining-convert-arch.png?raw=true "Call Recording Conversion Architecture Diagram")


# Deploying the project

The project code uses the Python version of the AWS CDK ([Cloud Development Kit](https://aws.amazon.com/cdk/)). To execute the project code, please ensure that you have fulfilled the [AWS CDK Prerequisites for Python](https://docs.aws.amazon.com/cdk/latest/guide/work-with-cdk-python.html).

The project code requires that the AWS account is [bootstrapped](https://docs.aws.amazon.com/de_de/cdk/latest/guide/bootstrapping.html) in order to allow the deployment of the CDK stack.

## Pre-requisites

2. Configure CDK context parameters in `cdk.context.json` found in the root directory

```
1.	bucket_name – Amazon S3 bucket name, where the Amazon Connect call recordings are stored (This can be found in Amazon Connect console(for your Connect instance) -> Data Storage -> Call recordings ).
2.	kms_key_arn – AWS KMS key ARN that is used by Amazon Connect to encrypt call recording files (This can be found in Amazon Connect console(for your Connect instance) -> Data Storage -> Call recordings -> Encrypted using this key).
3.	bucket_prefix – Amazon Connect call recording Path prefix (This can be found in Amazon Connect console(for your Connect instance) -> Data Storage -> Call recordings).
4.	num_days_age – Number of days ago to resample. Ex. 7. On a daily schedule it converts the call recordings older than current date – 7. For instance, if today was 15th of Feb, the workflow would not resample files between 15th of February and 8th of February, and it would only process files that are older than 8th of February.
5.	s3_storage_tier – Amazon S3 storage tier for resampled call recording files. Default: STANDARD. Options: GLACIER_IR, STANDARD_IA, ONEZONE_IA, INTELLIGENT_TIERING, GLACIER.

```
## Building the Lambda ffmpeg layer

This step downloads ffmpeg and places them in amazon-connect-call-recording-compression/lambda-layers/layer-ffmpeg folder.

cd amazon-connect-call-recording-cost-optimizer/lambda-layers/layer-ffmpeg
./build_layer_x86.sh

This will create the ffmpeg_layer.zip Lambda layer zip file.

## CDK Deployment

```
# navigate to project directory
cd amazon-connect-call-recording-cost-optimizer

# install and activate a Python Virtual Environment
python3 -m venv .venv
source .venv/bin/activate

# install dependant libraries
python -m pip install -r requirements.txt

```

### Bootstrap the account to setup CDK deployment

```
cdk bootstrap

```
### Upon successful completion of `cdk bootstrap`, the project is ready to be deployed.

```
cdk deploy 

```

### To deploy this stack, configure all required fields in cdk.context.json and run the following command which will install layers, install python dependencies, install cdk , bootstrap and deploy the cdk stack.

```
./cdk-deploy.sh

```


## Cleanup

When you’re finished experimenting with this solution, clean up your resources by running the command:

```
cdk destroy 

```

This command deletes resources there were deployed by AWS CDK. Amazon S3 bucket containing the call recordings and CloudWatch log groups are retained after the stack is deleted.

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This library is licensed under the MIT-0 License. See the LICENSE file.

