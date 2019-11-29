import boto3
import json
from jsonschema import validate
from jsonschema.exceptions import ValidationError
from spp.utils.logging import Logger

LOG = Logger(__name__).get()


def is_valid_json(instance, schema):
    """
    Calls the jsonschema library to test an instance of JSON against a schema definition. Exceptions are logged, but
    not raised.
    :param instance: JSON message
    :param schema: JSON definition dictionary
    :returns Boolean: True if valid JSON, else False
    """

    LOG.info(f"Event: {instance}")
    LOG.info("Validating JSON schema")

    try:
        validate(instance=instance, schema=schema)
    except ValidationError:
        LOG.exception("Validation failed")
        return False
    except Exception:
        LOG.exception("Non-validation exception raised during validation")
        return False
    else:
        LOG.info("Validation succeeded")
        return True


def write_queue(vendor_implementation, queue_resource, event):
    """
    Calls a given implementation to write to a message queue. Exceptions are logged, but not raised.
    :param vendor_implementation: Function that implements queue write
    :param queue_resource: Resource link for queue
    :param event: JSON message to write
    :returns response: Dictionary with queue response or error message
    """

    LOG.info(f"Writing to {queue_resource}")
    try:
        response = vendor_implementation(queue_resource, event)
    except Exception as ex:
        LOG.exception("Exception during message send")
        return {"Exception": "Message not sent", "Reason": str(ex)}
    else:
        LOG.info(f"Response: {response}")
        return response


def write_queue_sqs(sqs_queue, event):
    """
    Calls the boto3 client library to send a message to an SQS queue.
    :param sqs_queue: URL for SQS queue
    :param event: JSON message to write
    :returns response: Dictionary with SQS response or error message
    """

    client = boto3.client('sqs')
    return client.send_message(QueueUrl=sqs_queue, MessageBody=json.dumps(event))
