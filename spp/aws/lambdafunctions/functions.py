import os
import logging
import json
from message_utils import is_valid_json, write_queue


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def write_queue_handler(event, context=None):

    """
    Writes the event JSON to a message queue. Environment variables drive the variation.
    :param event: JSON message to be written
    :param context: Context dictionary
    :key QUEUE: Env-var with link to queue resource
    :key SCHEMA: Env-var with JSON format string with schema definition
    :key IMP_MODULE: Env-var with dot-delimited path to module of message queue write implementation
    :key IMP_METHOD: Env-var with name of implementing method
    :returns response: Dictionary with queue response or error message
    """
    
    # Get environment objects
    queue = os.environ['QUEUE']
    schema = json.loads(os.environ['SCHEMA'])
    imp_module = __import__(os.environ['IMP_MODULE'])
    imp_function = getattr(imp_module, os.environ['IMP_METHOD'])

    # Check JSON schema against expected shape
    if not is_valid_json(event, schema):
        return {"Exception": "Message not sent", "Reason": "JSON validation failed"}
    
    # Send message to SQS
    response = write_queue(imp_function, queue, event)
    return response
