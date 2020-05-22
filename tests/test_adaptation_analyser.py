from unittest.mock import patch, MagicMock

from event_service_utils.tests.base_test_case import MockedServiceStreamTestCase
from event_service_utils.tests.json_msg_helper import prepare_event_msg_tuple

from adaptation_analyser.service import AdaptationAnalyser

from adaptation_analyser.conf import (
    SERVICE_STREAM_KEY,
    SERVICE_CMD_KEY,
)


class TestAdaptationAnalyser(MockedServiceStreamTestCase):
    GLOBAL_SERVICE_CONFIG = {
        'service_stream_key': SERVICE_STREAM_KEY,
        'service_cmd_key': SERVICE_CMD_KEY,
        'logging_level': 'ERROR',
        'tracer_configs': {'reporting_host': None, 'reporting_port': None},
    }
    SERVICE_CLS = AdaptationAnalyser
    MOCKED_STREAMS_DICT = {
        SERVICE_STREAM_KEY: [],
        SERVICE_CMD_KEY: [],
    }

    @patch('adaptation_analyser.service.AdaptationAnalyser.process_action')
    def test_process_cmd_should_call_process_action(self, mocked_process_action):
        action = 'someAction'
        event_data = {
            'id': 1,
            'action': action,
            'some': 'stuff'
        }
        msg_tuple = prepare_event_msg_tuple(event_data)
        mocked_process_action.__name__ = 'process_action'

        self.service.service_cmd.mocked_values = [msg_tuple]
        self.service.process_cmd()
        self.assertTrue(mocked_process_action.called)
        self.service.process_action.assert_called_once_with(action=action, event_data=event_data, json_msg=msg_tuple[1])

    @patch('adaptation_analyser.service.AdaptationAnalyser.process_notify_changed_entity_action')
    def test_process_action_should_call_process_notify_changed_entity_action(self, mocked_notify_entity):
        action = 'notifyChangedEntity'
        change_type = 'addEntity'
        entity_type = 'gnosis-mep:buffer_stream'
        event_data = {
            'id': '123',
            'entity': {
                '@type': entity_type
            },
            'action': action,
            'change_type': change_type
        }
        msg_tuple = prepare_event_msg_tuple(event_data)
        json_msg = msg_tuple[1]
        self.service.process_action(action, event_data, json_msg)

        self.assertTrue(mocked_notify_entity.called)
        self.service.process_notify_changed_entity_action.assert_called_once_with(event_data, change_type, entity_type)

    def test_process_notify_changed_entity_action_should_call_correct_funcs_correctly(self):
        action = 'notifyChangedEntity'
        change_type = 'addEntity'
        entity_type = 'gnosis-mep:buffer_stream'
        event_data = {
            'id': '123',
            'entity': {
                '@type': entity_type
            },
            'action': action,
            'change_type': change_type
        }

        func1 = MagicMock(__name__='mocked_func1', return_value='func_1_ret')
        func2 = MagicMock(__name__='mocked_func2', return_value='func_2_ret')
        self.service.entity_type_to_processing_functions_map[entity_type] = [func1, func2]
        self.service.process_notify_changed_entity_action(event_data, change_type, entity_type)

        self.assertTrue(func1.called)
        self.assertTrue(func2.called)
        func1.assert_called_once_with(event_data, change_type, None)
        func2.assert_called_once_with(event_data, change_type, 'func_1_ret')

    @patch('adaptation_analyser.service.AdaptationAnalyser.analyse_buffer_stream_change')
    def test_process_notify_changed_entity_action_should_call_correct_funcs_for_buffer_stream_added(self, mocked_as_bs_change):
        action = 'notifyChangedEntity'
        change_type = 'addEntity'
        entity_type = 'gnosis-mep:buffer_stream'
        event_data = {
            'id': '123',
            'entity': {
                '@type': entity_type
            },
            'action': action,
            'change_type': change_type
        }
        mocked_as_bs_change.__name__ = 'mocked_as_bs_change'
        self.service.entity_type_to_processing_functions_map[entity_type] = [mocked_as_bs_change]

        self.service.process_notify_changed_entity_action(event_data, change_type, entity_type)

        mocked_as_bs_change.assert_called_once_with(event_data, change_type, None)
