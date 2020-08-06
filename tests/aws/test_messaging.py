from spp.aws.messaging import is_valid_json, write_queue


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


def test_write_queue_working_implementation():

    resource = "https://link/to/queue:0000"
    event = {
        "method1": {
            "param1": 1,
            "param2": "2"
        }
    }

    def implementation(resource, event):
        return {resource: event}

    response = write_queue(implementation, resource, event)
    assert response == {resource: event}


def test_write_queue_exception_raised():

    resource = "https://link/to/queue:0000"
    event = {
        "method1": {
            "param1": 1,
            "param2": "2"
        }
    }

    def implementation(resource, event):
        raise Exception("broken implementation")

    response = write_queue(implementation, resource, event)
    assert response == {'Exception': 'Message not sent', 'Reason': 'broken implementation'}