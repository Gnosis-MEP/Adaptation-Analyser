#!/usr/bin/env python
import uuid
import json
from event_service_utils.streams.redis import RedisStreamFactory

from adaptation_analyser.conf import (
    REDIS_ADDRESS,
    REDIS_PORT,
    SERVICE_STREAM_KEY,
    SERVICE_CMD_KEY,
)


def make_dict_key_bites(d):
    return {k.encode('utf-8'): v for k, v in d.items()}


def new_action_msg(action, event_data):
    event_data['action'] = action
    event_data.update({'id': str(uuid.uuid4())})
    return {'event': json.dumps(event_data)}


def send_action_msgs(service_cmd):
    msg_1 = new_action_msg(
        'someAction',
        {
            'some': 'event',
            'data': 'to be used'
        }
    )
    msg_2 = new_action_msg(
        'someOtherAction',
        {
            'some': 'other event',
            'data': 'to be used'
        }
    )

    print(f'Sending msg {msg_1}')
    service_cmd.write_events(msg_1)
    print(f'Sending msg {msg_2}')
    service_cmd.write_events(msg_2)


def send_data_msg(service_stream):
    data_msg = {
        'event': json.dumps(
            {
                'id': str(uuid.uuid4()),
                'some': 'data'
            }
        )
    }
    print(f'Sending msg {data_msg}')
    service_stream.write_events(data_msg)


def main():
    stream_factory = RedisStreamFactory(host=REDIS_ADDRESS, port=REDIS_PORT)
    service_cmd = stream_factory.create(SERVICE_CMD_KEY, stype='streamOnly')
    service_stream = stream_factory.create(SERVICE_STREAM_KEY, stype='streamOnly')
    import ipdb; ipdb.set_trace()
    send_action_msgs(service_cmd)
    send_data_msg(service_stream)


if __name__ == '__main__':
    main()
