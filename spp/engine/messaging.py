import logging
import boto3
import json
from jsonschema import validate
from jsonschema.exceptions import ValidationError


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class QueueWriter:


class SQSQueueWriter(QueueWriter):


def write_queue(vendor_implementation, queue_resource, event):

    """
    Calls a given implementation to write to a message queue. Exceptions are logged, but not raised.
    :param vendor_implementation: Function that implements queue write
    :param queue_resource: Resource link for queue
    :param event: JSON message to write
    :returns response: Dictionary with queue response or error message
    """

    logging.info(f"Writing to {queue_resource}")
    try:
        response = vendor_implementation(queue_resource, event)
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
