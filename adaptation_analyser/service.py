import datetime
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
            ],
            'gnosis-mep:service_worker': [
                self.analyse_service_worker_change,
                self.analyse_service_worker_best_idle,
            ],
            'gnosis-mep:subscriber_query': [
                self.analyse_subscriber_query_change,
            ]
        }
        self.knowledge_cmd_stream_key = 'adpk-cmd'
        self.planner_cmd_stream_key = 'adpp-cmd'
        self.knowledge_cmd_stream = self.stream_factory.create(key=self.knowledge_cmd_stream_key, stype='streamOnly')
        self.planner_cmd_stream = self.stream_factory.create(key=self.planner_cmd_stream_key, stype='streamOnly')

        self.min_seconds_to_ask_same_change_request_type = 3
        self.recent_plan_change_requests_timestamps = {}
        self.min_queue_space_percent = 0.3

        self.has_received_subquery = False
        self.has_received_bufferstream = False

    def _workaround_for_query_and_bufferstream_race_condition(self, event_type):
        if event_type == 'subscriber_query':
            self.has_received_subquery = True
        if event_type == 'bufferstream':
            self.has_received_bufferstream = True

        ret = self.has_received_bufferstream and self.has_received_subquery
        # if received both, than should reset so that a new one can be validated
        if ret:
            self.has_received_bufferstream = False
            self.has_received_subquery = False
        return ret

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
        self.recent_plan_change_requests_timestamps[event_data['change']['type']] = datetime.datetime.now().timestamp()
        self.logger.info(f'Sending Change Plan Request to Planner: {event_data}')
        self.write_event_with_trace(event_data, self.planner_cmd_stream)

    def verify_service_worker_overloaded(self, event_data, min_queue_space_percent):
        json_ld_entity = event_data['entity']
        entity_graph = json_ld_entity['@graph']
        overloaded_workers = []
        for service_worker in entity_graph:
            if service_worker['gnosis-mep:service_worker#queue_space_percent'] < min_queue_space_percent:
                overloaded_workers.append(service_worker)

        return overloaded_workers

    def verify_service_worker_best_idle(self, event_data):
        json_ld_entity = event_data['entity']
        entity_graph = json_ld_entity['@graph']
        best_worker_by_service = {}
        for service_worker in entity_graph:
            energy_consumption = service_worker.get('gnosis-mep:service_worker#energy_consumption')
            if energy_consumption is None:
                continue
            energy_consumption = float(energy_consumption)
            worker_key = service_worker['gnosis-mep:service_worker#stream_key']
            service_type = service_worker['gnosis-mep:service_worker#service_type']
            queue_space_percent = int(service_worker['gnosis-mep:service_worker#queue_space_percent'])
            best_worker_for_service_type = best_worker_by_service.get(service_type, None)
            if best_worker_for_service_type and energy_consumption >= best_worker_for_service_type[1]:
                continue
            best_worker_for_service_type = (worker_key, energy_consumption, queue_space_percent)
            best_worker_by_service[service_type] = best_worker_for_service_type

        # filter out the workers that dont have all space left available
        # that is, only get the ones that have 1 as the queue_space_percent
        best_idle_workers_by_service = dict(filter(lambda x: x[1][2] == 1, best_worker_by_service.items()))
        return best_idle_workers_by_service

    def verify_dont_have_similar_recent_plan_in_execution(self, event_data, change_plan_request_type, extra_time=0):
        # should check on K the current plans and their timestamp to ignore any plan that's too recent
        last_request_timestamp = self.recent_plan_change_requests_timestamps.get(change_plan_request_type)
        if not last_request_timestamp:
            return True

        ts_now = datetime.datetime.now().timestamp()
        seconds_since_last_request = ts_now - last_request_timestamp
        min_time = self.min_seconds_to_ask_same_change_request_type + extra_time
        if seconds_since_last_request < min_time:
            return False

        return True

    def analyse_service_worker_change(self, event_data, change_type, last_func_ret=None):
        change_plan_request_type = 'serviceWorkerOverloaded'
        if change_type == 'updateEntity':
            if not self.verify_dont_have_similar_recent_plan_in_execution(event_data, change_plan_request_type):
                self.logger.info(
                    f'Ignoring "{change_plan_request_type}" because other similar plan was executed too rencently'
                )
                return

            overloaded_workers = self.verify_service_worker_overloaded(event_data, self.min_queue_space_percent)
            if len(overloaded_workers) != 0:
                event_change_plan_data = self.build_change_plan_request_data(
                    change_type=change_plan_request_type, change_cause=event_data['entity']
                )
                self.send_change_request_for_planner(event_change_plan_data)
                return event_change_plan_data

    def analyse_service_worker_best_idle(self, event_data, change_type, last_func_ret=None):
        change_plan_request_type = 'serviceWorkerBestIdle'

        if last_func_ret is not None:
            self.logger.info(f'Ignoring best idle worker analysis, since a previous plan was already prepared.')
            return

        if change_type == 'updateEntity':
            if not self.verify_dont_have_similar_recent_plan_in_execution(event_data, change_plan_request_type, extra_time=1):
                self.logger.info(
                    f'Ignoring "{change_plan_request_type}" because other similar plan was executed too rencently'
                )
                return
            best_idle_workers_by_service = self.verify_service_worker_best_idle(event_data)
            if len(best_idle_workers_by_service.keys()) != 0:
                event_change_plan_data = self.build_change_plan_request_data(
                    change_type=change_plan_request_type, change_cause=event_data['entity']
                )
                self.send_change_request_for_planner(event_change_plan_data)
                return event_change_plan_data

    def analyse_buffer_stream_change(self, event_data, change_type, last_func_ret=None):
        if change_type == 'addEntity':
            # Should only send the request if we received already a query change event
            # otherwise it means that this info will not be available for the planner yet.
            # the same thing will occur for the query change.
            if self._workaround_for_query_and_bufferstream_race_condition('bufferstream'):
                event_change_plan_data = self.build_change_plan_request_data(
                    change_type='incorrectSchedulerPlan', change_cause=event_data['entity']
                )
                self.send_change_request_for_planner(event_change_plan_data)
            return

    def analyse_subscriber_query_change(self, event_data, change_type, last_func_ret=None):
        if change_type == 'addEntity':
            # Should only send the request if we received already a buffer stream change event
            # otherwise it means that this info will not be available for the planner yet.
            # the same thing will occur for the query change.
            if self._workaround_for_query_and_bufferstream_race_condition('subscriber_query'):
                event_change_plan_data = self.build_change_plan_request_data(
                    change_type='incorrectSchedulerPlan', change_cause=event_data['entity']
                )
                self.send_change_request_for_planner(event_change_plan_data)
            return

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
        elif action == 'notifyChangedEntityGraph':
            json_ld_entity = event_data['entity']
            entity_graph = json_ld_entity['@graph']
            entity_type = entity_graph[0]['@type']
            change_type = event_data['change_type']
            self.process_notify_changed_entity_action(event_data, change_type, entity_type)

    def log_state(self):
        super(AdaptationAnalyser, self).log_state()
        self.logger.info(f'My service name is: {self.name}')
        self._log_dict('Recent Change Requests', self.recent_plan_change_requests_timestamps)

    def run(self):
        super(AdaptationAnalyser, self).run()
        self.cmd_thread = threading.Thread(target=self.run_forever, args=(self.process_cmd,))
        self.data_thread = threading.Thread(target=self.run_forever, args=(self.process_data,))
        self.cmd_thread.start()
        self.data_thread.start()
        self.cmd_thread.join()
        self.data_thread.join()
