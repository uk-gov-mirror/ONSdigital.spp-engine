import boto3
import json
import logging
from jsonschema import validate
from jsonschema.exceptions import ValidationError


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class QueueWriter:

    def send_message(self, queue, event, **kwargs):
        """ To be implemented by child classes"""
        raise NotImplementedError('Abstract method')

    def __repr__(self):
        return 'QueueWriter'


class SQSQueueWriter(QueueWriter):

    def __init__(self):
        self.client = boto3.client('sqs')

    def send_message(self, queue, event, **kwargs):
        """
        Calls the boto3 client library to send a message to an SQS queue.
        :param queue: URL for SQS queue
        :param event: JSON message to write
        :returns response: Dictionary with SQS response or error message
        """
        return self.client.send_message(QueueUrl=queue, MessageBody=json.dumps(event), **kwargs)


def write_queue(queue_resource, event, writer=QueueWriter(), **kwargs):
    """
    Writes a message to a queue. A QueueWriter instance must be supplied which implements send_message().
    :param queue_resource: String link to queue
    :param event: JSON message
    :param writer: connection object that extends QueueWriter
    :param kwargs: Other keyword arguments to pass to implementing method
    :return:
    """
    _message_log(queue_resource, writer)
    try:
        response = writer.send_message(queue_resource, event, **kwargs)
    except NotImplementedError as ex:
        logging.exception("send_message method in writer instance not implemented.")
        raise
    except Exception as ex:
        logging.exception("Exception during message send")
        return {"Exception": "Message not sent", "Reason": str(ex)}
    else:
        logging.info(f"Response: {response}")
        return response


def is_valid_json(instance, schema):

    """
    Calls the jsonschema library to test an instance of JSON against a schema definition. Exceptions are logged, but
    not raised.
    :param instance: JSON message
    :param schema: JSON definition dictionary
    :returns Boolean: True if valid JSON, else False
    """

    logging.info(f"Event: {instance}")
    logging.info("Validating JSON schema")

    try:
        validate(instance=instance, schema=schema)
    except ValidationError:
        logging.exception("Validation failed")
        return False
    except Exception:
        logging.exception("Non-validation exception raised during validation")
        return False
    else:
        logging.info("Validation succeeded")
        return True


def _message_log(queue_resource, writer):
    logger.info(f"Writing to {queue_resource}")
    logger.info(f"Writer: {writer}")
