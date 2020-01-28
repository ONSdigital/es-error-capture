import json
import logging
import os

import boto3
from botocore.exceptions import ClientError
from es_aws_functions import aws_functions, exception_classes
from marshmallow import Schema, fields


class EnvironSchema(Schema):
    sns_topic_arn = fields.Str(required=True)


def send_sns_message(error_message, arn):
    """
    This method is responsible for sending a notification
    to the specified arn, so that it can be
    used to relay information for the BPM to use and handle.
    :param error_message: An error message. - Type: String.
    :param arn: Arn of the topic to send message to. - Type: String.

    :return: None
    """
    sns = boto3.client('sns', region_name='eu-west-2')
    sns_message = {
        "success": False,
        "message": error_message
    }

    return sns.publish(
        TargetArn=arn,
        Message=json.dumps(sns_message)
    )


def lambda_handler(event, context):
    current_module = "Error Capture"
    # Define run_id outside of try block
    run_id = 0
    logger = logging.getLogger("Error capture")
    logger.setLevel(10)
    log_message = ''
    error_message = ''
    try:
        logger.info("Entered Error Handler")
        # Retrieve run_id before input validation
        # Because it is used in exception handling
        run_id = event['run_id']
        queue_url = event["queue_url"]

        schema = EnvironSchema()
        config, errors = schema.load(os.environ)
        if errors:
            raise ValueError(f"Error validating environment params: {errors}")
        # Environment variables
        sns_topic_arn = config['sns_topic_arn']

        # Set up client
        sqs = boto3.client('sqs', region_name='eu-west-2')

        # Take the error message from event
        runtime_error_message = event['Cause']
        logger.info("Retrieved error message")

        # send on to sns
        send_sns_message(runtime_error_message, sns_topic_arn)
        logger.info("Sent error to sns topic")

        # Delete queue
        sqs.delete_queue(QueueUrl=queue_url)
        logger.info("Deleted: " + str(queue_url))

    except ClientError as e:
        error_message = ("AWS Error in ("
                         + str(e.response["Error"]["Code"]) + ") "
                         + current_module + " |- "
                         + str(e.args)
                         + " | Run_id: " + str(run_id))

        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)

    except KeyError as e:
        error_message = ("Key Error in "
                         + current_module + " |- "
                         + str(e.args)
                         + " | Run_id: " + str(run_id)
                         )

        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)

    except ValueError as e:
        error_message = ("Blank or empty environment variable in "
                         + current_module + " |- "
                         + str(e.args)
                         + " | Run_id: " + str(run_id))

        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)
    except Exception as e:
        error_message = ("General Error in "
                         + current_module + " ("
                         + str(type(e)) + ") |- "
                         + str(e.args)
                         + " | Run_id: " + str(run_id))

        log_message = error_message + " | Line: " + str(e.__traceback__.tb_lineno)
    finally:
        if (len(error_message)) > 0:
            logger.error(log_message)
            raise exception_classes.LambdaFailure(error_message)
