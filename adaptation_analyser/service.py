import threading

from event_service_utils.logging.decorators import timer_logger
from event_service_utils.services.tracer import BaseTracerService
from event_service_utils.tracing.jaeger import init_tracer


class AdaptationAnalyser(BaseTracerService):
    def __init__(self,
                 service_stream_key, service_cmd_key,
                 stream_factory,
                 logging_level,
                 tracer_configs):
        tracer = init_tracer(self.__class__.__name__, **tracer_configs)
        super(AdaptationAnalyser, self).__init__(
            name=self.__class__.__name__,
            service_stream_key=service_stream_key,
            service_cmd_key=service_cmd_key,
            stream_factory=stream_factory,
            logging_level=logging_level,
            tracer=tracer,
        )
        self.cmd_validation_fields = ['id', 'action']
        self.data_validation_fields = ['id']

        self.entity_type_to_processing_functions_map = {
            'gnosis-mep:buffer_stream': [
                self.analyse_buffer_stream_change
            ]
        }
        self.knowledge_cmd_stream_key = 'adpk-cmd'
        self.planner_cmd_stream_key = 'adpp-cmd'
        self.knowledge_cmd_stream = self.stream_factory.create(key=self.knowledge_cmd_stream_key, stype='streamOnly')
        self.planner_cmd_stream = self.stream_factory.create(key=self.planner_cmd_stream_key, stype='streamOnly')

    # def send_event_to_somewhere(self, event_data):
    #     self.logger.debug(f'Sending event to somewhere: {event_data}')
    #     self.write_event_with_trace(event_data, self.somewhere_stream)

    def build_change_plan_request_data(self, change_type, change_cause):
        event_change_plan_data = {
            'id': self.service_based_random_event_id(),
            'action': 'changePlanRequest',
            'change': {
                'type': change_type,
                'cause': change_cause
            }
        }
        return event_change_plan_data

    def send_change_request_for_planner(self, event_data):
        self.logger.info('Sending Change Plan Request to Planner: {event_data}')
        self.write_event_with_trace(event_data, self.planner_cmd_stream)

    def analyse_buffer_stream_change(self, event_data, change_type, last_func_ret=None):
        if change_type == 'addEntity':
            # every time a buffer stream is added we need to update the scheduler to handle it
            event_change_plan_data = self.build_change_plan_request_data(
                change_type='incorrectSchedulerPlan', change_cause=event_data['entity']
            )
            self.send_change_request_for_planner(event_change_plan_data)
            return
        else:
            pass

    @timer_logger
    def process_data_event(self, event_data, json_msg):
        if not super(AdaptationAnalyser, self).process_data_event(event_data, json_msg):
            return False
        # do something here
        pass

    def process_notify_changed_entity_action(self, event_data, change_type, entity_type):
        processing_functions = self.entity_type_to_processing_functions_map.get(entity_type)
        if processing_functions is None:
            self.logger.error(f'No analysis is available for entity type: {entity_type}. Ignoring...')
        last_func_ret = None
        for func in processing_functions:
            self.logger.debug(f'Execution analysis of {event_data} on {func.__name__}...')
            last_func_ret = func(event_data, change_type, last_func_ret)
        return last_func_ret

    def process_action(self, action, event_data, json_msg):
        if not super(AdaptationAnalyser, self).process_action(action, event_data, json_msg):
            return False
        if action == 'notifyChangedEntity':
            json_ld_entity = event_data['entity']
            entity_type = json_ld_entity['@type']
            change_type = event_data['change_type']
            self.process_notify_changed_entity_action(event_data, change_type, entity_type)
        elif action == 'otherAction':
            # do some other action
            pass

    def log_state(self):
        super(AdaptationAnalyser, self).log_state()
        self.logger.info(f'My service name is: {self.name}')

    def run(self):
        super(AdaptationAnalyser, self).run()
        self.cmd_thread = threading.Thread(target=self.run_forever, args=(self.process_cmd,))
        self.data_thread = threading.Thread(target=self.run_forever, args=(self.process_data,))
        self.cmd_thread.start()
        self.data_thread.start()
        self.cmd_thread.join()
        self.data_thread.join()
