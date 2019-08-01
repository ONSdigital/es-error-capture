import os  # noqa F401
from moto import mock_sqs, mock_sns
import boto3
import unittest.mock as mock
import unittest
import sys

import time
sys.path.append(os.path.realpath(os.path.dirname(__file__) + "/.."))
import runtime_error_capture  # noqa E402


class TestRuntimeErrorCapture(unittest.TestCase):

    def test_get_traceback(self):
        traceback = runtime_error_capture._get_traceback(Exception("Mike"))
        assert traceback == "Exception: Mike\n"

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
                {"arn": topic_arn, "queue_url": queue_url}
        ):
            indata = {
                  "data": {
                    "lambdaresult": {
                      "error": "Oh no, an error!"
                    }
                  }
                }

            runtime_error_capture.lambda_handler(indata, "")
            time.sleep(6)
            message = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10)
            assert('messages' not in message)

    @mock_sqs
    def test_marshmallow_raises_exception(self):
        sqs = boto3.resource("sqs", region_name="eu-west-2")
        sqs.create_queue(QueueName="test_queue")
        queue_url = sqs.get_queue_by_name(QueueName="test_queue").url
        # Method
        with mock.patch.dict(
                runtime_error_capture.os.environ, {"queue_url": queue_url}
        ):
            out = runtime_error_capture.lambda_handler(
                {"RuntimeVariables": {"checkpoint": 666}}, None
            )
            self.assertRaises(ValueError)
            assert(out['error'].__contains__
                   ("""ValueError: Error validating environment params:"""))

    @mock_sqs
    def test_catch_method_exception(self):
        # Method

        with mock.patch.dict(
                runtime_error_capture.os.environ,
                {
                "arn": "Arrgh",
                "queue_url": "responder_id"
                 },
        ):
            with mock.patch("runtime_error_capture.logging.info") as mocked:
                mocked.side_effect = Exception("SQS Failure")
                response = runtime_error_capture.lambda_handler(
                    "", None
                )
                assert "success" in response
                assert response["success"] is False
