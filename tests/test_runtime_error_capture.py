import unittest
import unittest.mock as mock

import boto3
from es_aws_functions import exception_classes
from moto import mock_sns, mock_sqs

import runtime_error_capture  # noqa E402


class TestRuntimeErrorCapture(unittest.TestCase):

    @mock_sns
    def test_sns_send(self):
        with mock.patch.dict(
                runtime_error_capture.os.environ, {"arn": "mike"}
        ):
            sns = boto3.client("sns", region_name="eu-west-2")
            topic = sns.create_topic(Name="bloo")
            topic_arn = topic["TopicArn"]

            result = runtime_error_capture.send_sns_message("", topic_arn)
            assert(result['ResponseMetadata']['HTTPStatusCode'] == 200)

    @mock_sns
    @mock_sqs
    def test_method(self):
        sqs = boto3.client("sqs", region_name="eu-west-2")
        sqs.create_queue(QueueName="test_queue")
        queue_url = sqs.get_queue_url(QueueName="test_queue")['QueueUrl']
        sqs.send_message(
            QueueUrl=queue_url,
            MessageBody="moo",
            MessageGroupId="123",
            MessageDeduplicationId="666"
        )
        sns = boto3.client("sns", region_name="eu-west-2")
        topic = sns.create_topic(Name="bloo")
        topic_arn = topic["TopicArn"]
        with mock.patch.dict(
                runtime_error_capture.os.environ,
                {}
        ):
            indata = {"error": {"Cause": "Bad stuff"},
                      "run_id": "moo",
                      "queue_url": queue_url,
                      "sns_topic_arn": topic_arn
                      }

            runtime_error_capture.lambda_handler(indata, "")
            error = ''
            try:
                sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10)
            except Exception as e:
                error = e.args
                # Extract e for use in finally block
                # so if it doesnt throw exception test will fail
            finally:

                assert "The specified queue does not exist" in str(error)

    @mock_sqs
    def test_marshmallow_raises_exception(self):
        sqs = boto3.resource("sqs", region_name="eu-west-2")
        sqs.create_queue(QueueName="test_queue")
        queue_url = sqs.get_queue_by_name(QueueName="test_queue").url

        with unittest.TestCase.assertRaises(
                self, exception_classes.LambdaFailure) as exc_info:
            runtime_error_capture.lambda_handler(
                {"error": {"Cause": "Bad stuff"},
                 "run_id": "moo",
                 "queue_url": queue_url,
                 "sns_topic_arn": "topic_arn"
                 }, None
            )
        assert "Error validating environment" \
               in exc_info.exception.error_message

    @mock_sqs
    def test_catch_method_exception(self):
        # Method

        with mock.patch.dict(
                runtime_error_capture.os.environ,
                {},
        ):
            with mock.patch("runtime_error_capture.boto3.client") as mocked:
                mocked.side_effect = Exception("SQS Failure")
                with unittest.TestCase.assertRaises(
                        self, exception_classes.LambdaFailure) as exc_info:
                    runtime_error_capture.lambda_handler(
                        {"Cause": "Bad stuff",
                         "run_id": "moo",
                         "queue_url": "abc",
                         "sns_topic_arn": "topic_arn"
                        }, None
                    )
                assert "General Error" \
                       in exc_info.exception.error_message
