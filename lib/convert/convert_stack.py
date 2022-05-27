import json
from constructs import Construct
from aws_cdk import App, Stack,Duration, Stack

from aws_cdk import(
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as sfn_tasks,
    aws_lambda as _lambda,
    aws_events as events,
    aws_events_targets as targets,
    aws_iam as iam,
    aws_sqs as _sqs,
    aws_lambda_destinations as _lambda_dest,
    aws_s3 as s3,
    Duration
)

from aws_cdk.aws_lambda_event_sources import SqsEventSource

from aws_cdk.aws_stepfunctions import (
    JsonPath
)
class CallRecordStack(Stack):
    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        CONNECT_BUCKET = self.node.try_get_context("bucket_name")
        
        CONNECT_KMSKEY_ARN = self.node.try_get_context("kms_key_arn")

        CONNECT_BUCKET_PREFIX = self.node.try_get_context("bucket_prefix")
        
        NUM_DAYS_AGE = self.node.try_get_context("num_days_age")
        
        S3_STORAGE_TIER = self.node.try_get_context("s3_storage_tier")
    
        OVERWRITE_PREVIOUS_CONVERTED = self.node.try_get_context("overwrite_previous_converted")
    
        
        ##############################################################################
        # Job Queue for Converstions
        ##############################################################################

        # Create the SQS queue visibility_timeout 6x convert lambda timeout
        
        dead_letter_queue = _sqs.Queue(self, "connect_audio_convert_dlq",
            queue_name=f'connect_audio_convert_dlq'
        )
        
        queue =  _sqs.Queue(self, "connect_audio_convert",
            queue_name=f'connect_audio_convert',
            visibility_timeout=Duration.minutes(90),
            dead_letter_queue=_sqs.DeadLetterQueue(
                max_receive_count=5,
                queue=dead_letter_queue
            )
            )
  
        ##############################################################################
        # Lambda Queue for Conversion Failures
        ##############################################################################

        lambda_convert_dest_failure_queue = _sqs.Queue(
            self,
            "connect_audio_convert_dest_failure_queue",
            queue_name=f'connect_audio_convert_dest_failure_queue'
        )
        
        ##############################################################################
        # Lambdas and layers 
        ##############################################################################
        
        # x86 ffmpeg layer
        ffmpeg_layer = _lambda.LayerVersion(
            self, "ffmpeg_layer",
            code=_lambda.AssetCode('lambda-layers/layer-ffmpeg/ffmpeg_layer.zip'),
            description="x86 ffmpeg layer",
            compatible_runtimes=[_lambda.Runtime.PYTHON_3_9],
            compatible_architectures=[_lambda.Architecture.X86_64])

        ##############################################################################
        # Policies for Lambda
        ##############################################################################

        S3ReadWritePolicyStmt = iam.PolicyStatement(
                                resources=["arn:aws:s3:::" + CONNECT_BUCKET ,"arn:aws:s3:::" + CONNECT_BUCKET + "/*"],
                                actions=[
                                    'S3:ListBucket',
                                    'S3:PutObjectTagging',
                                    'S3:GetObjectTagging',
                                    'S3:ListBucket',
                                    'S3:PutObject',
                                    'S3:GetObject',
                                ]
                            )
        
        S3ReadPolicyStmt = iam.PolicyStatement(
                                resources=["arn:aws:s3:::" + CONNECT_BUCKET ,"arn:aws:s3:::" + CONNECT_BUCKET + "/*"],
                                actions=[
                                    'S3:ListBucket',
                                    'S3:GetObjectTagging',
                                    'S3:ListBucket',
                                    'S3:GetObject',
                                ]
                            )
        S3ReadKMSPolicyStmt = iam.PolicyStatement(
                                resources=[CONNECT_KMSKEY_ARN],
                                actions=[
                                    'kms:Decrypt',
                                    'kms:Encrypt',
                                    'kms:GenerateDataKey',
                                ]
                            )    
                            

        ##############################################################################
        # Creating Lambda Convert function that will be triggered by the SQS Queue 
        ##############################################################################

        convert_lambda = _lambda.Function(self,'convert',
            function_name=f'media-convert-files',
            handler='lambda-handler.lambda_handler',
            runtime=_lambda.Runtime.PYTHON_3_9,
            code=_lambda.Code.from_asset('lambdas/convert'),
            description="converts audio with ffmpeg",
            timeout=Duration.seconds(900),
            memory_size=800,
            environment = {
                'CONNECT_RECORDING_S3_BUCKET': CONNECT_BUCKET,
                'PREFIX': CONNECT_BUCKET_PREFIX,
                'S3_STORAGE_TIER':S3_STORAGE_TIER
                },
            layers=[ffmpeg_layer],
            on_failure=_lambda_dest.SqsDestination(lambda_convert_dest_failure_queue),
            )


        #Add SQS event source to the Lambda function
        convert_lambda.add_event_source(SqsEventSource(queue,
            batch_size=1,
        ))

        # Add inline policy to the lambda
        convert_lambda.add_to_role_policy(S3ReadWritePolicyStmt)
        convert_lambda.add_to_role_policy(S3ReadKMSPolicyStmt)

        # Add Permissions to lambda to write messags to queue
        lambda_convert_dest_failure_queue.grant_send_messages(convert_lambda)
        

        ##############################################################################
        # Creating iterator Lambda function that will be add S3 objects to convert
        ##############################################################################

        step_iterator_lambda = _lambda.Function(self,'step-call-recording-iterator',
            function_name=f'step-call-recording-iterator',
            handler='lambda-handler.lambda_handler',
            runtime=_lambda.Runtime.PYTHON_3_9,
            code=_lambda.Code.from_asset('lambdas/iterator-step'),
            description="iterates s3 bucket prefix for files to convert",
            timeout=Duration.seconds(900),
            environment = {
            'CONNECT_RECORDING_S3_BUCKET': CONNECT_BUCKET,
            'PREFIX': CONNECT_BUCKET_PREFIX,
            'MAX_KEYS':'1000',
            'NUM_DAYS_AGE': NUM_DAYS_AGE, 
            }
        )


        # Add inline policy to the lambda
        step_iterator_lambda.add_to_role_policy(S3ReadWritePolicyStmt)
        step_iterator_lambda.add_to_role_policy(S3ReadKMSPolicyStmt)

        
        ##############################################################################
        # Creating Queue Lambda function that will be add S3 objects to convert
        ##############################################################################

        step_queue_lambda = _lambda.Function(self,'step-place-call-recording-in-queue',
            function_name=f'step-place-call-recording-in-queue',
            handler='lambda-handler.lambda_handler',
            runtime=_lambda.Runtime.PYTHON_3_9,
            code=_lambda.Code.from_asset('lambdas/iterator-queue'),
            description="adds files to queue for conversion",
            timeout=Duration.seconds(900),
            environment = {
            'CONNECT_RECORDING_CONVERT_QUEUE': queue.queue_url,
            'CONNECT_RECORDING_S3_BUCKET': CONNECT_BUCKET,
            'OVERWRITE_PREVIOUS_CONVERTED': OVERWRITE_PREVIOUS_CONVERTED
            }
        )

        # Add inline policy to the lambda
        step_queue_lambda.add_to_role_policy(S3ReadWritePolicyStmt)
        step_queue_lambda.add_to_role_policy(S3ReadKMSPolicyStmt)

        # Add Permissions to lambda to write messags to queue
        lambda_convert_dest_failure_queue.grant_send_messages(step_queue_lambda)
        queue.grant_send_messages(step_queue_lambda)

        ##############################################################################
        # Step Function
        ##############################################################################

        succeed_nothing_to_job = sfn.Succeed(
            self, "No files to convert.",
            comment='Job succeeded'
        )
        succeed_convert_job = sfn.Succeed(
            self, "Queue is empty.",
            comment='Convert Job succeeded'
        )

        configure = sfn.Pass(
            self, 
            "configure",
            parameters={
                "NextContinuationToken": "",
                "specific_date.$": "$$.Execution.Input.specific_date"
            },
            result_path="$.iterator"
        )

        iterator = sfn_tasks.LambdaInvoke(
            self, "7 days ago recordings s3 iterator",
            input_path="$.iterator",
            lambda_function=step_iterator_lambda,
            result_selector={
                "iterator": {
                    "files":sfn.JsonPath.string_at("$.Payload.files"),
                    "NextContinuationToken": sfn.JsonPath.string_at("$.Payload.NextContinuationToken"),
                    "specific_date.$": "$$.Execution.Input.specific_date"
                }
            }
        )

        get_queue_length = sfn_tasks.CallAwsService(self, "Check the convert queue length",
            service="sqs",
            action="getQueueAttributes",
            iam_resources=[queue.queue_arn],
            parameters={
                "QueueUrl": queue.queue_url,
                "AttributeNames": [
                "ApproximateNumberOfMessages",
                "ApproximateNumberOfMessagesDelayed",
                "ApproximateNumberOfMessagesNotVisible"
                ]
            }
        )

        wait_60m=sfn.Wait(self,"Wait 60 Seconds",
            time=sfn.WaitTime.duration(Duration.seconds(60)))

        convert_recordings = sfn_tasks.LambdaInvoke(
            self, "Add files to convert Queue",
            input_path="$",
            result_path=JsonPath.DISCARD,
            lambda_function=step_queue_lambda,
        )
    
        catch_convert_error = sfn.Pass(
            self,
            "catch an error",
            result_path=JsonPath.DISCARD
        )

        # catch error and retry
        convert_recordings.add_catch(catch_convert_error, errors=['States.ALL'],result_path='$.error')
        convert_recordings.add_retry(backoff_rate=1.05,interval=Duration.seconds(5),errors=["ConvertRetry"])
        

        # check the sqs queue
        wait_60m.next(get_queue_length).next(sfn.Choice(self, 'Queue length?')\
            .when(sfn.Condition.string_equals('$.Attributes.ApproximateNumberOfMessagesDelayed', '0'), succeed_convert_job)\
            .otherwise(wait_60m)
            .afterwards())
            
        # convert steps 
        definition = configure.next(iterator)\
            .next(sfn.Choice(self, 'Has Files To Process?')\
            .when(sfn.Condition.string_equals('$.iterator.files', ''), succeed_nothing_to_job)\
            .otherwise(convert_recordings)\
            .afterwards())\
            .next(sfn.Choice(self, 'More files to process?')\
            .when(sfn.Condition.string_equals('$.iterator.NextContinuationToken', ''), wait_60m)\
            .otherwise(iterator))
            
        # set default empty specific day which then uses the NUM_DAYS_AGE delta from current date
        json_input = "{\"specific_date\": \"\"}"

        step_event_parameter = events.RuleTargetInput.from_object(json.loads(json_input))
        
        # Create state machine
        sm = sfn.StateMachine(
            self, "media-batchconvert-stepfunction",
            state_machine_name =f'media-batchconvert',
            definition=definition,
            timeout=Duration.minutes(1800),
            tracing_enabled=True,
        )
 
        
        ##############################################################################
        # EventBridge Trgger Cron
        ##############################################################################

        scheduling_rule = events.Rule(
            self, "Scheduling Rule",
            rule_name=f'recording-convert-sqs-SchedulingRule',
            description="Daily triggered event to convert call recordings.",
            schedule=events.Schedule.cron(
                #minute='0/1440',
                hour='23',
                # month='*',
                # week_day='*',
                # year='*'
                ),
        )
        
        scheduling_rule.add_target(targets.SfnStateMachine(sm,input = step_event_parameter))        
        
        