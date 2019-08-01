import json
import boto3
import traceback
import os
from marshmallow import Schema, fields
import logging

# Set up clients
sns = boto3.client('sns', region_name='eu-west-2')
sqs = boto3.client('sqs', region_name='eu-west-2')


class EnvironSchema(Schema):
    queue_url = fields.Str(required=True)
    arn = fields.Str(required=True)


def _get_traceback(exception):
    """
    Given an exception, returns the traceback as a string.
    :param exception: Exception object
    :return: string
    """
    return ''.join(
        traceback.format_exception(
            etype=type(exception), value=exception, tb=exception.__traceback__
        )
    )


def send_sns_message(error_message, arn):
    """
    This method is responsible for sending a notification
    to the specified arn, so that it can be
    used to relay information for the BPM to use and handle.
    :param error_message: An error message. - Type: String.
    :param arn: Arn of the topic to send message to. - Type: String.

    :return: None
    """

    sns_message = {
        "success": False,
        "message": error_message
    }

    return sns.publish(
        TargetArn=arn,
        Message=json.dumps(sns_message)
    )


def lambda_handler(event, context):
    try:
        logger = logging.getLogger("Error_capture")
        logger.info("Entered Error Handler")

        schema = EnvironSchema()
        config, errors = schema.load(os.environ)
        if errors:
            raise ValueError(f"Error validating environment params: {errors}")
        # Environment variables
        arn = config['arn']
        queue_url = config["queue_url"]

        # Take the error message from event
        error_message = event['data']['lambdaresult']['error']
        logger.info("Retrieved error message")
        # send on to sns
        send_sns_message(error_message, arn)
        logger.info("Sent error to sns topic")
        # Purge contents of queue
        sqs.purge_queue(QueueUrl=queue_url)
        logger.info("Purged Queue")
    except Exception as exc:
        logger.error("Error Handler has failed.")
        return {
            "success": False,
            "error": "Unexpected exception {}".format(_get_traceback(exc))
        }
