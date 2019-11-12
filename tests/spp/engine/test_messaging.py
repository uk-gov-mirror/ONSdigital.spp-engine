from spp.engine.messaging import is_valid_json, write_queue, QueueWriter, SQSQueueWriter
from unittest.mock import patch


valid_schema = {
    "type": "object",
    "properties": {
        "method1": {
            "type": "object",
            "properties": {
                "param1": {"type": "number"},
                "param2": {"type": "string"}
            },
            "required": ["param1", "param2"],
            "additionalProperties": False,
        },
        "method2": {
            "type": "object",
            "properties": {
                "param1": {"type": "number"},
                "param2": {"type": "string"}
            },
            "required": ["param1", "param2"],
            "additionalProperties": False,
        }
    },
    "required": ["method1", "method2"],
    "additionalProperties": False,
}


def test_valid_json():

    instance = {
        "method1": {
            "param1": 1,
            "param2": "2"
        },
        "method2": {
            "param1": 9,
            "param2": "hi"
        }
    }

    assert is_valid_json(instance, valid_schema)


def test_json_missing_required_property():

    instance = {
        "method1": {
            "param1": 1,
            "param2": "2"
        }
    }

    assert not is_valid_json(instance, valid_schema)


def test_json_with_extra_property():

    instance = {
        "method1": {
            "param1": 1,
            "param2": "2"
        },
        "method2": {
            "param1": 9,
            "param2": "hi"
        },
        "method3": {
            "param1": 123,
        }
    }

    assert not is_valid_json(instance, valid_schema)


def test_garbled_json():

    instance = {
        'method1': [{
            'param1': [{}],
            'param2': '2'
        }],
        'method2': []
    }

    assert not is_valid_json(instance, valid_schema)


@patch('spp.engine.messaging.SQSQueueWriter.send_message')
def test_write_queue_working_implementation(mock_instance_method):

    resource = "https://link/to/queue:0000"
    event = {
        "method1": {
            "param1": 1,
            "param2": "2"
        }
    }

    mock_instance_method.return_value = {resource: event}
    response = write_queue(resource, event, SQSQueueWriter())
    assert response == {resource: event}


@patch('spp.engine.messaging.SQSQueueWriter.send_message')
def test_write_queue_broken_implementation(mock_instance_method):

    resource = "https://link/to/queue:0000"
    event = {
        "method1": {
            "param1": 1,
            "param2": "2"
        }
    }

    mock_instance_method.side_effect = Exception("broken implementation")
    response = write_queue(resource, event, SQSQueueWriter())
    assert response == {'Exception': 'Message not sent', 'Reason': 'broken implementation'}


def test_write_queue_not_implemented():

    resource = "https://link/to/queue:0000"
    event = {
        "method1": {
            "param1": 1,
            "param2": "2"
        }
    }

    raised = False
    try:
        write_queue(resource, event, QueueWriter())
    except NotImplementedError:
        raised = True
    finally:
        assert raised
