"""CDK Stack for the AC Trace Exporter infrastructure.

Deploys:
- Lambda function (with bundled dependencies) for Firehose data transformation
- Kinesis Data Firehose delivery stream with the Lambda as a processor
- CloudWatch Logs subscription filter on the aws/spans log group
- S3 bucket for Firehose backup/dead-letter delivery
- Required IAM roles and permissions
"""

import shutil
import subprocess
import sys
from pathlib import Path

import aws_cdk as cdk
import jsii
from aws_cdk import (
    BundlingOptions,
    DockerImage,
    Duration,
    ILocalBundling,
    RemovalPolicy,
    aws_iam as iam,
    aws_kinesisfirehose as firehose,
    aws_lambda as lambda_,
    aws_logs as logs,
    aws_s3 as s3,
)
from constructs import Construct

LAMBDA_DIR = str(Path(__file__).resolve().parent.parent.parent / "lambda")


@jsii.implements(ILocalBundling)
class LocalBundling:
    """Local bundling that installs Python deps via pip without Docker."""

    def try_bundle(self, output_dir: str, *, image: DockerImage, **kwargs) -> bool:
        lambda_path = Path(LAMBDA_DIR)

        # Copy source files
        for item in lambda_path.iterdir():
            dest = Path(output_dir) / item.name
            if item.is_dir():
                shutil.copytree(str(item), str(dest))
            else:
                shutil.copy2(str(item), str(dest))

        # Install requirements
        requirements = lambda_path / "requirements.txt"
        if requirements.exists():
            pip_commands = [
                ["uv", "pip", "install", "--python", "python3",
                 "-r", str(requirements), "--target", output_dir],
                [sys.executable, "-m", "pip",
                 "install", "-r", str(requirements), "-t", output_dir],
            ]
            for cmd in pip_commands:
                result = subprocess.run(cmd, capture_output=True, text=True)
                if result.returncode == 0:
                    return True
            # Fall back to Docker bundling
            return False

        return True


class TraceExporterStack(cdk.Stack):
    """Stack: CW Logs -> Firehose -> Lambda processor -> OTLP endpoint."""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Context values (configurable at deploy time via cdk.json or --context)
        otlp_endpoint = self.node.try_get_context("otlp_endpoint") or ""
        otlp_headers = self.node.try_get_context("otlp_headers") or ""
        filter_attributes = (
            self.node.try_get_context("filter_span_attributes")
            or "aws.xray.origin=AWS::BedrockAgentCore::*"
        )
        log_group_name = (
            self.node.try_get_context("log_group_name") or "aws/spans"
        )

        # ------- S3 Bucket - Firehose backup destination -------
        backup_bucket = s3.Bucket(
            self,
            "FirehoseBackupBucket",
            removal_policy=RemovalPolicy.RETAIN,
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            lifecycle_rules=[
                s3.LifecycleRule(
                    expiration=Duration.days(30),
                    id="expire-after-30-days",
                )
            ],
        )

        # ------- Lambda Function - Firehose processor -------
        bundling_image = DockerImage.from_registry(
            "public.ecr.aws/lambda/python:3.12"
        )

        processor_lambda = lambda_.Function(
            self,
            "TraceProcessorLambda",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="handler.handler",
            code=lambda_.Code.from_asset(
                LAMBDA_DIR,
                bundling=BundlingOptions(
                    image=bundling_image,
                    command=[
                        "bash",
                        "-c",
                        (
                            "pip install -r requirements.txt -t /asset-output"
                            " && cp -au . /asset-output"
                        ),
                    ],
                    local=LocalBundling(),
                ),
            ),
            timeout=Duration.minutes(5),
            memory_size=512,
            environment={
                "OTEL_EXPORTER_OTLP_ENDPOINT": otlp_endpoint,
                "OTEL_EXPORTER_OTLP_HEADERS": otlp_headers,
                "FILTER_SPAN_ATTRIBUTES": filter_attributes,
                "DRY_RUN": self.node.try_get_context("dry_run") or "true",
            },
            description=(
                "Processes Firehose records: filters spans "
                "and exports to OTLP endpoint"
            ),
        )

        # ------- IAM Role for Firehose -------
        firehose_role = iam.Role(
            self,
            "FirehoseRole",
            assumed_by=iam.ServicePrincipal("firehose.amazonaws.com"),
        )

        backup_bucket.grant_read_write(firehose_role)
        processor_lambda.grant_invoke(firehose_role)

        # ------- Kinesis Data Firehose Delivery Stream -------
        delivery_stream = firehose.CfnDeliveryStream(
            self,
            "TraceExporterDeliveryStream",
            delivery_stream_name="ac-trace-exporter-stream",
            delivery_stream_type="DirectPut",
            extended_s3_destination_configuration=(
                firehose.CfnDeliveryStream.ExtendedS3DestinationConfigurationProperty(
                    bucket_arn=backup_bucket.bucket_arn,
                    role_arn=firehose_role.role_arn,
                    buffering_hints=(
                        firehose.CfnDeliveryStream.BufferingHintsProperty(
                            interval_in_seconds=60,
                            size_in_m_bs=1,
                        )
                    ),
                    compression_format="GZIP",
                    prefix=(
                        "traces/year=!{timestamp:yyyy}/"
                        "month=!{timestamp:MM}/"
                        "day=!{timestamp:dd}/"
                    ),
                    error_output_prefix=(
                        "errors/year=!{timestamp:yyyy}/"
                        "month=!{timestamp:MM}/"
                        "day=!{timestamp:dd}/"
                        "!{firehose:error-output-type}/"
                    ),
                    processing_configuration=(
                        firehose.CfnDeliveryStream.ProcessingConfigurationProperty(
                            enabled=True,
                            processors=[
                                firehose.CfnDeliveryStream.ProcessorProperty(
                                    type="Lambda",
                                    parameters=[
                                        firehose.CfnDeliveryStream.ProcessorParameterProperty(
                                            parameter_name="LambdaArn",
                                            parameter_value=processor_lambda.function_arn,
                                        ),
                                        firehose.CfnDeliveryStream.ProcessorParameterProperty(
                                            parameter_name="BufferSizeInMBs",
                                            parameter_value="1",
                                        ),
                                        firehose.CfnDeliveryStream.ProcessorParameterProperty(
                                            parameter_name="BufferIntervalInSeconds",
                                            parameter_value="60",
                                        ),
                                    ],
                                )
                            ],
                        )
                    ),
                )
            ),
        )
        # Firehose needs its role policy (S3 + Lambda invoke) before creation
        delivery_stream.node.add_dependency(firehose_role)

        # ------- IAM Role for CloudWatch Logs subscription filter -------
        cw_logs_role = iam.Role(
            self,
            "CWLogsToFirehoseRole",
            assumed_by=iam.ServicePrincipal("logs.amazonaws.com"),
        )

        cw_logs_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "firehose:PutRecord",
                    "firehose:PutRecordBatch",
                ],
                resources=[delivery_stream.attr_arn],
            )
        )

        # ------- CloudWatch Logs Subscription Filter -------
        subscription_filter = logs.CfnSubscriptionFilter(
            self,
            "SpansSubscriptionFilter",
            log_group_name=log_group_name,
            destination_arn=delivery_stream.attr_arn,
            filter_pattern="",  # Empty pattern matches all events
            role_arn=cw_logs_role.role_arn,
            filter_name="ac-trace-exporter-subscription",
        )
        # Ensure Firehose is ACTIVE and IAM is fully propagated
        subscription_filter.add_dependency(delivery_stream)
        subscription_filter.node.add_dependency(cw_logs_role)

        # ------- Outputs -------
        cdk.CfnOutput(
            self,
            "DeliveryStreamArn",
            value=delivery_stream.attr_arn,
            description="ARN of the Firehose delivery stream",
        )
        cdk.CfnOutput(
            self,
            "ProcessorLambdaArn",
            value=processor_lambda.function_arn,
            description="ARN of the trace processor Lambda",
        )
        cdk.CfnOutput(
            self,
            "BackupBucketName",
            value=backup_bucket.bucket_name,
            description="S3 bucket for Firehose backup delivery",
        )
