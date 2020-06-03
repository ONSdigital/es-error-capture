import json
import unittest
from unittest import mock

import pandas as pd
import pytest
from es_aws_functions import exception_classes, test_generic_library
from moto import mock_sns, mock_sqs
import boto3

import runtime_error_capture as lambda_wrangler_function

method_runtime_variables = {
    "run_id": "bob",
    "sns_topic_arn": "topic_arn",
    "queue_url": "mock_url",
    "error": {
        "Error": "LambdaFailure",
        "Cause": "{\"errorMessage\": \"<class 'ValueError'> tested_for_error]}"
    },
    "RuntimeVariables": {}
}

bad_runtime_variables = {
    "run_id": "bob",
    "sns_topic_arn": "",
    "queue_url": "",
    "error": {
        "Error": "LambdaFailure",
        "Cause": "{\"errorMessage\": \"<class 'ValueError'> tested_for_error]}"
    }
}


##########################################################################################
#                                     Generic                                            #
##########################################################################################


@pytest.mark.parametrize(
    "which_lambda,which_runtime_variables,which_environment_variables,"
    "which_data,expected_message,assertion",
    [
        (lambda_wrangler_function, method_runtime_variables,
         None, None,
         "ClientError", test_generic_library.wrangler_assert)
    ])
def test_client_error(which_lambda, which_runtime_variables,
                      which_environment_variables, which_data,
                      expected_message, assertion):
    test_generic_library.client_error(which_lambda, which_runtime_variables,
                                      which_environment_variables, which_data,
                                      expected_message, assertion)


@pytest.mark.parametrize(
    "which_lambda,which_runtime_variables,which_environment_variables,mockable_function,"
    "expected_message,assertion",
    [
        (lambda_wrangler_function, method_runtime_variables,
         None, "runtime_error_capture.RuntimeSchema",
         "'Exception'", test_generic_library.wrangler_assert)
    ])
def test_general_error(which_lambda, which_runtime_variables,
                       which_environment_variables, mockable_function,
                       expected_message, assertion):
    test_generic_library.general_error(which_lambda, which_runtime_variables,
                                       which_environment_variables, mockable_function,
                                       expected_message, assertion)


@pytest.mark.parametrize(
    "which_lambda,which_environment_variables,expected_message,assertion",
    [
        (lambda_wrangler_function, None,
         "KeyError", test_generic_library.wrangler_assert)
    ])
def test_key_error(which_lambda, which_environment_variables,
                   expected_message, assertion):
    test_generic_library.key_error(which_lambda, which_environment_variables,
                                   expected_message, assertion)


@pytest.mark.parametrize(
    "which_lambda,expected_message,assertion,which_runtime_variables",
    [(lambda_wrangler_function,
      "Error validating environment parameters",
        test_generic_library.wrangler_assert, bad_runtime_variables)])
def test_value_error(which_lambda, expected_message, assertion, which_runtime_variables):
    test_generic_library.value_error(which_lambda, expected_message, assertion,
                                     runtime_variables=bad_runtime_variables)

##########################################################################################
#                                     Specific                                           #
##########################################################################################


@mock_sns
def test_send_sns_message():
    sns = boto3.client("sns", region_name="eu-west-2")
    topic = sns.create_topic(Name="bloo")
    topic_arn = topic["TopicArn"]

    result = lambda_wrangler_function.send_sns_message("", topic_arn)
    assert(result["ResponseMetadata"]["HTTPStatusCode"] == 200)


@mock_sns
@mock_sqs
@pytest.mark.parametrize(
    "which_lambda,which_runtime_variables,expected_message",
    [
        (lambda_wrangler_function, method_runtime_variables,
         "The specified queue does not exist")
    ])
def test_runtime_error_capture(which_lambda, which_runtime_variables, expected_message):
    sqs = boto3.client("sqs", region_name="eu-west-2")
    sqs.create_queue(QueueName="test_queue")
    queue_url = sqs.get_queue_url(QueueName="test_queue")["QueueUrl"]
    which_runtime_variables["queue_url"] = queue_url

    sqs.send_message(
        QueueUrl=queue_url,
        MessageBody="moo",
        MessageGroupId="123",
        MessageDeduplicationId="666"
    )
    sns = boto3.client("sns", region_name="eu-west-2")
    topic = sns.create_topic(Name="bloo")
    topic_arn = topic["TopicArn"]

    which_runtime_variables["sns_topic_arn"] = topic_arn

    which_lambda.lambda_handler(which_runtime_variables, "")
    error = ''
    try:
        sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10)
    except Exception as e:
        error = e.args
        # Extract e for use in finally block
        # so if it doesnt throw exception test will fail
    finally:
        assert expected_message in str(error)


@mock_sqs
@mock.patch("runtime_error_capture.boto3.client")
@pytest.mark.parametrize(
    "which_lambda,which_runtime_variables,expected_msg",
    [
        (lambda_wrangler_function, method_runtime_variables,
         "tested_for_error")
    ])
def test_runtime_error_capture_success(mock_client, which_lambda,
                                       which_runtime_variables, expected_msg):

    with mock.patch("runtime_error_capture.send_sns_message") as mocked_sns_queue:
        mocked_sns_queue.return_value = "NotARealSNS"
        which_lambda.lambda_handler(which_runtime_variables, None)
        response = mocked_sns_queue.call_args

    assert expected_msg in response[0][0]
