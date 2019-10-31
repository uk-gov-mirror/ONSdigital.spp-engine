import logging
import boto3
import json
from jsonschema import validate
from jsonschema.exceptions import ValidationError


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


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


def send_message_sqs(sqs_queue, event):

    """
    Calls the boto3 client library to send a message to an SQS queue. Exceptions are logged, but not raised.
    :param sqs_queue: URL for SQS queue
    :param event: JSON message to write
    :returns response: Dictionary with SQS response or error message
    """

    logging.info(f"Writing to {sqs_queue}")
    client = boto3.client('sqs')

    try:
        response = client.send_message(
            QueueUrl=sqs_queue,
            MessageBody=json.dumps(event))
    except Exception as ex:
        logging.exception("SQS Exception during message send")
        return {"Exception": "Message not sent", "Reason": str(ex)}
    else:
        logging.info(f"Response: {response}")
        return response
