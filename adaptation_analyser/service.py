import datetime
import math
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
        self.adaptation_delta = 10
        self.best_workers_by_service_by_qos_policy = {}
        self.query_qos_policies = self.prepare_query_qos_policies()
        self.has_received_subquery = False
        self.has_received_bufferstream = False
        self.number_of_workers = 0

    def prepare_query_qos_policies(self):
        query_qos_policies = {
            'energy_consumption=min': {
                'worker_policy_key': 'gnosis-mep:service_worker#energy_consumption',
            },
            'latency=min': {
                'worker_policy_key': 'gnosis-mep:service_worker#throughput',
            },
            'accuracy=max': {
                'worker_policy_key': 'gnosis-mep:service_worker#accuracy',
            },
        }

        def comp_lower_than(a_val, b_val):
            return a_val < b_val

        def comp_higher_than(a_val, b_val):
            return a_val > b_val

        for qos_policy, policy_data in query_qos_policies.items():
            if '=min' in qos_policy and 'energy_consumption' in qos_policy:
                comparison = comp_lower_than
            else:
                comparison = comp_higher_than
            # policy_data['worker_a_b_comparison'] = comparison
            query_qos_policies[qos_policy]['worker_a_b_comparison'] = comparison
        return query_qos_policies

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

    def _is_service_worker_overloaded(self, service_worker):
        queue_size = int(service_worker['gnosis-mep:service_worker#queue_size'])
        throughput = float(service_worker['gnosis-mep:service_worker#throughput'])
        capacity = math.floor(throughput * self.adaptation_delta)
        return capacity > queue_size

    def verify_service_worker_overloaded(self, event_data, min_queue_space_percent):
        json_ld_entity = event_data['entity']
        entity_graph = json_ld_entity['@graph']
        overloaded_workers = list(filter(lambda w: self._is_service_worker_overloaded(w), entity_graph))

        return overloaded_workers

    def get_best_worker_by_service_by_qos_policy(self, entity_graph):
        tracked_policies = set(sorted(self.best_workers_by_service_by_qos_policy.keys()))
        current_policies = set(sorted(self.query_qos_policies.keys()))
        has_new_policy = tracked_policies != current_policies
        has_new_worker = len(entity_graph) != self.number_of_workers
        if has_new_policy or has_new_worker:
            for qos_policy, policy_data in self.query_qos_policies.items():
                if qos_policy in tracked_policies and not has_new_worker:
                    continue

                for service_worker in entity_graph:
                    service_type = service_worker['gnosis-mep:service_worker#service_type']

                    #'gnosis-mep:service_worker#energy_consumption'
                    worker_policy_key = policy_data['worker_policy_key']
                    worker_policy_value = service_worker.get(worker_policy_key)
                    if worker_policy_value is None:
                        continue

                    worker_policy_value = float(worker_policy_value)

                    qos_policy_best_workers = self.best_workers_by_service_by_qos_policy.setdefault(qos_policy, {})

                    best_worker_for_service_type = qos_policy_best_workers.get(service_type, None)
                    worker_policy_comparison = policy_data['worker_a_b_comparison']
                    has_best_worker = best_worker_for_service_type is not None
                    if has_best_worker:
                        best_worker_policy_value = best_worker_for_service_type[worker_policy_key]
                        is_bestter_than = worker_policy_comparison(worker_policy_value, best_worker_policy_value)
                        if not is_bestter_than:
                            continue
                    qos_policy_best_workers[service_type] = service_worker
                    self.best_workers_by_service_by_qos_policy[qos_policy] = qos_policy_best_workers
            self.number_of_workers = len(entity_graph)
        return self.best_workers_by_service_by_qos_policy

    def _is_worker_idle(self, service_worker):
        queue_size = int(service_worker['gnosis-mep:service_worker#queue_size'])
        return queue_size == 0

    def update_best_worker_by_service_by_qos_policy(self, event_data):
        json_ld_entity = event_data['entity']
        entity_graph = json_ld_entity['@graph']
        self.get_best_worker_by_service_by_qos_policy(entity_graph)

    def verify_service_worker_best_idle(self, event_data):
        json_ld_entity = event_data['entity']
        entity_graph = json_ld_entity['@graph']

        idle_workers_keys = map(lambda f: f['gnosis-mep:service_worker#stream_key'], filter(
            lambda sw: self._is_worker_idle(sw),
            entity_graph
        ))

        # if all workers are idle than it doesn't matter
        # because there isn't actually any input to be done
        if len(idle_workers_keys) == len(entity_graph):
            return False

        best_workers_keys = set([])

        for qos_policy in self.query_qos_policies.keys():
            for service_worker in self.get_best_worker_by_service_by_qos_policy(entity_graph)[qos_policy].values():
                worker_key = service_worker['gnosis-mep:service_worker#stream_key']
                best_workers_keys.add(worker_key)

        has_idle_best_worker = any([best_w_key in idle_workers_keys for best_w_key in best_workers_keys])
        return has_idle_best_worker

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
        self.update_best_worker_by_service_by_qos_policy(event_data)
        if last_func_ret is not None:
            self.logger.info(f'Ignoring best idle worker analysis, since a previous plan was already prepared.')
            return

        if change_type == 'updateEntity':
            if not self.verify_dont_have_similar_recent_plan_in_execution(event_data, change_plan_request_type, extra_time=1):
                self.logger.info(
                    f'Ignoring "{change_plan_request_type}" because other similar plan was executed too rencently'
                )
                return
            has_idle_best_worker = self.verify_service_worker_best_idle(event_data)
            if has_idle_best_worker:
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
        self._log_dict('Best Workers by service by QOS policy', self.best_workers_by_service_by_qos_policy)

    def run(self):
        super(AdaptationAnalyser, self).run()
        self.cmd_thread = threading.Thread(target=self.run_forever, args=(self.process_cmd,))
        self.cmd_thread.start()
        self.cmd_thread.join()
