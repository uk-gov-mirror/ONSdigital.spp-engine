import os
import logging
import json
from message_utils import is_valid_json, send_message_sqs


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def write_sqs(event, context=None):

    """
    Writes the event JSON to an Amazon SQS queue. Exceptions are logged, but not raised. Configuration is driven by
    environment variables SQS_QUEUE and VALID_SCHEMA.
    :param event: JSON message to be written
    :param context: Lambda context dictionary
    :returns response: Dictionary with SQS response or error message
    """
    
    # Get environment objects
    sqs_queue = os.environ['SQS_QUEUE']  # Environment variable with URL string
    valid_schema = json.loads(os.environ['VALID_SCHEMA'])  # Environment variable with JSON-format string
    
    # Check JSON schema against expected shape
    if not is_valid_json(event, valid_schema):
                return {"Exception": "Message not sent", "Reason": "JSON validation failed"}
    
    # Send message to SQS
    response = send_message_sqs(sqs_queue, event)
    
    return response
