
import json
import gzip
import base64
import arrow
import pytest
from function import handler

@pytest.fixture
def event_maker():
    """
    raw_data is a gzip-compressed, base64-encoded representation of this structure
    {
      "messageType": "DATA_MESSAGE",
      "owner": "542186135646",
      "logGroup": "/aws/lambda/foo-zoom-ingester-zoom-foo-function",
      "logStream": "2018/06/19/[33]abcd1234",
      "subscriptionFilters": [
        "jluker-zoom-ingester-ZoomUploaderLogSubscriptionFilter-SJXIJ5HTP9QJ"
      ],
      "logEvents": [
        {
          "id": "12345",
          "timestamp": 1529421105754,
          "message": "{\"hello\": \"world!\"}"
        },
        {
          "id": "67890",
          "timestamp": 1529421105756,
          "message": "{\"foo\": \"bar\", \"baz\": 54}",
          "exception": "\nTraceback (most recent call last):\n  File \"<doctest...>\", line 10, in <module>\n    lumberjack()\n  File \"<doctest...>\", line 4, in lumberjack\n    bright_side_of_death()\nIndexError: tuple index out of range\n"
        }
      ]
    }
    """
    def func(*msgs):
        log_events = []
        for idx, msg in enumerate(msgs, 1):
            log_events.append({
                'id': idx,
                'timestamp': arrow.now().timestamp * 1000,
                'message': msg
            })

        data = { 'logEvents': log_events }
        uncompressed = json.dumps(data)
        compressed = gzip.compress(uncompressed.encode())
        encoded = base64.b64encode(compressed)
        return { 'awslogs': { 'data': encoded } }
    return func

def test_non_event(mocker, event_maker):
    mock_publisher = mocker.patch('function.CWPublisher')
    event = event_maker("this is a log message!")
    handler(event, None)
    assert mock_publisher.publish_wf_duration_metric.call_count == 0
    assert mock_publisher.publish_op_duration_metric.call_count == 0

def test_wf_event(mocker, event_maker):
    mock_publisher = mocker.patch('function.CWPublisher')
    event = event_maker("Workflow 12345 state set to SUCCEEDED")
    handler(event, None)
    assert mock_publisher.publish_wf_duration_metric.call_count == 1


