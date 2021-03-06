import json
import logging

import boto3
from es_aws_functions import exception_classes, general_functions
from marshmallow import EXCLUDE, Schema, fields


class ErrorSchema(Schema):
    Cause = fields.Str(required=True)
    Error = fields.Str(required=True)


class RuntimeSchema(Schema):
    class Meta:
        unknown = EXCLUDE

    def handle_error(self, e, data, **kwargs):
        logging.error(f"Error validating runtime params: {e}")
        raise ValueError(f"Error validating runtime params: {e}")

    environment = fields.Str(required=True)
    error = fields.Nested(ErrorSchema, required=True)
    sns_topic_arn = fields.Str(required=True)
    survey = fields.Str(required=True)


def lambda_handler(event, context):
    current_module = "Error Capture"
    # Define run_id outside of try block.
    run_id = 0
    error_message = ''
    try:
        # Retrieve run_id before input validation
        # Because it is used in exception handling.
        run_id = event['RuntimeVariables']["run_id"]

        runtime_variables = RuntimeSchema().load(event['RuntimeVariables'])
        # Runtime variables.
        environment = runtime_variables["environment"]
        error = runtime_variables["error"]
        sns_topic_arn = runtime_variables["sns_topic_arn"]
        survey = runtime_variables["survey"]

    except Exception as e:
        error_message = general_functions.handle_exception(e, current_module,
                                                           run_id, context)
        raise exception_classes.LambdaFailure(error_message)

    try:
        logger = general_functions.get_logger(survey, current_module, environment,
                                              run_id)
    except Exception as e:
        error_message = general_functions.handle_exception(e, current_module,
                                                           run_id, context)
        raise exception_classes.LambdaFailure(error_message)

    try:
        logger.info("Started - retrieved configuration variables.")

        # Take the error message from event.
        runtime_error_message = error["Cause"]
        logger.info("Retrieved error message.")

        # Call send_sns_message.
        send_sns_message(runtime_error_message, sns_topic_arn)
        logger.info("Sent error to sns topic.")

    except Exception as e:
        error_message = general_functions.handle_exception(e, current_module,
                                                           run_id, context)
    finally:
        if (len(error_message)) > 0:
            logger.error(error_message)
            raise exception_classes.LambdaFailure(error_message)

    logger.info("Successfully completed module.")


def send_sns_message(error_message, arn):
    """
    This method is responsible for sending a notification
    to the specified arn, so that it can be
    used to relay information for the BPM to use and handle.
    :param error_message: An error message. - Type: String.
    :param arn: Arn of the topic to send message to. - Type: String.

    :return: None
    """
    sns = boto3.client("sns", region_name="eu-west-2")
    sns_message = {
        "success": False,
        "message": error_message
    }

    return sns.publish(
        TargetArn=arn,
        Message=json.dumps(sns_message)
    )
