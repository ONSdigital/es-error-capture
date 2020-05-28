import json
from unittest import mock

import pandas as pd
import pytest
from es_aws_functions import exception_classes, test_generic_library
from moto import mock_s3
from pandas.util.testing import assert_frame_equal

import runtime_error_capture as lambda_wrangler_function

method_runtime_variables = {
    "RuntimeVariables": {
        "data": None,
        "run_id": "bob",
        "sns_topic_arn": "mock_arn",
        "error": "error",
        "queue_url": "mock_url"

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
    "which_lambda,which_environment_variables,expected_message,assertion",
    [
        (lambda_wrangler_function, None,
         "KeyError", test_generic_library.wrangler_assert)
    ])
def test_key_error(which_lambda, which_environment_variables,
                   expected_message, assertion):
    test_generic_library.key_error(which_lambda, which_environment_variables,
                                   expected_message, assertion)



##########################################################################################
#                                     Specific                                           #
##########################################################################################
