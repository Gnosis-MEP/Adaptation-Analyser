import datetime
import math
import threading

from event_service_utils.logging.decorators import timer_logger
from event_service_utils.services.event_driven import BaseEventDrivenCMDService
from event_service_utils.tracing.jaeger import init_tracer


class AdaptationAnalyser(BaseEventDrivenCMDService):
    def __init__(self,
                 service_stream_key, service_cmd_key_list,
                 pub_event_list, service_details,
                 stream_factory,
                 logging_level,
                 tracer_configs):
        tracer = init_tracer(self.__class__.__name__, **tracer_configs)
        super(AdaptationAnalyser, self).__init__(
            name=self.__class__.__name__,
            service_stream_key=service_stream_key,
            service_cmd_key_list=service_cmd_key_list,
            pub_event_list=pub_event_list,
            service_details=service_details,
            stream_factory=stream_factory,
            logging_level=logging_level,
            tracer=tracer,
        )
        self.cmd_validation_fields = ['id']
        self.data_validation_fields = ['id']

        # self.entity_type_to_processing_functions_map = {
        #     'gnosis-mep:buffer_stream': [
        #         self.analyse_buffer_stream_change
        #     ],
        #     'gnosis-mep:service_worker': [
        #         self.analyse_service_worker_overloaded,
        #         self.analyse_service_worker_best_idle,
        #         self.analyse_unnecessary_load_shedding,
        #     ],
        #     'gnosis-mep:subscriber_query': [
        #         self.analyse_subscriber_query_change,
        #     ]
        # }

        self.current_service_workers = {}
        self.min_seconds_to_ask_same_change_request_type = 3
        self.recent_plan_change_requests_timestamps = {}
        self.adaptation_delta = 10
        self.best_workers_by_service_by_qos_policy = {}
        self.query_qos_policies = self.prepare_query_qos_policies()

        self.last_service_workers_monitoring = None
        self.number_of_workers = 0
        self.current_plan = None
        self.overloaded_workers = None
        self.is_overloaded_percentage = 0.7

    def prepare_query_qos_policies(self):
        query_qos_policies = {
            'energy_consumption=min': {
                'worker_policy_attr': 'energy_consumption',
            },
            'latency=min': {
                'worker_policy_attr': 'throughput',
            },
            'accuracy=max': {
                'worker_policy_attr': 'accuracy',
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
            query_qos_policies[qos_policy]['worker_a_b_comparison'] = comparison
        return query_qos_policies

    # def verify_dont_have_similar_recent_plan_in_execution(self, event_data, change_plan_request_type, extra_time=0):
    #     # should check on K the current plans and their timestamp to ignore any plan that's too recent
    #     last_request_timestamp = self.recent_plan_change_requests_timestamps.get(change_plan_request_type)
    #     if not last_request_timestamp:
    #         return True

    #     ts_now = datetime.datetime.now().timestamp()
    #     seconds_since_last_request = ts_now - last_request_timestamp
    #     min_time = self.min_seconds_to_ask_same_change_request_type + extra_time
    #     if seconds_since_last_request < min_time:
    #         return False

    #     return True

    def update_current_plan(self, plan):
        self.current_plan = plan

    def build_change_plan_request_data(self, event_type, change_cause):
        event_change_plan_data = {
            'id': self.service_based_random_event_id(),
            'change': {
                'type': event_type,
                'cause': change_cause
            }
        }
        return event_change_plan_data

    def process_query_created(self, event_data):
        event_type = 'NewQuerySchedulingPlanRequested'
        event_change_plan_data = self.build_change_plan_request_data(
            event_type=event_type, change_cause=event_data
        )
        self.publish_event_type_to_stream(event_type=event_type, new_event_data=event_change_plan_data)

    def update_best_worker_by_service_by_qos_policy(self, service_workers):
        for qos_policy, policy_data in self.query_qos_policies.items():
            for service_type, service_type_dict in service_workers.items():
                for worker, worker_data in service_type_dict['workers'].items():

                    worker_policy_attr = policy_data['worker_policy_attr']
                    worker_policy_value = worker_data.get(worker_policy_attr)
                    if worker_policy_value is None:
                        continue

                    worker_policy_value = float(worker_policy_value)

                    qos_policy_best_workers = self.best_workers_by_service_by_qos_policy.setdefault(qos_policy, {})

                    best_worker_for_service_type = qos_policy_best_workers.get(service_type, None)
                    worker_policy_comparison = policy_data['worker_a_b_comparison']
                    has_best_worker = best_worker_for_service_type is not None
                    if has_best_worker:
                        best_worker_policy_value = best_worker_for_service_type[worker_policy_attr]
                        is_better_than = worker_policy_comparison(worker_policy_value, best_worker_policy_value)
                        if not is_better_than:
                            continue
                    qos_policy_best_workers[service_type] = worker_data
                    self.best_workers_by_service_by_qos_policy[qos_policy] = qos_policy_best_workers
        return self.best_workers_by_service_by_qos_policy

    def process_service_worker_announced(self, event_data):
        worker = event_data.get('worker')
        stream_key = worker.get('stream_key')
        service_type = worker.get('service_type')
        service_type_dict = self.current_service_workers.setdefault(service_type, {})
        workers_dict = service_type_dict.setdefault('workers', {})
        workers_dict[stream_key] = worker
        self.update_best_worker_by_service_by_qos_policy(self.current_service_workers)

    def _is_service_worker_overloaded(self, service_worker):
        queue_size = int(service_worker.get('queue_size', 0))
        throughput = float(service_worker.get('throughput', 0.0))
        capacity = math.floor(throughput * self.adaptation_delta)
        if capacity == 0:
            return True
        overloaded_percentage = queue_size / capacity

        return overloaded_percentage >= self.is_overloaded_percentage

    def verify_service_workers_overloaded(self, event_data):
        overloaded_workers = []
        service_workers = event_data.get('service_workers', {})
        for service_type_dict in service_workers.values():
            service_overloaded_workers = []
            for worker, worker_data in service_type_dict['workers'].items():
                if self._is_service_worker_overloaded(worker_data):
                    service_overloaded_workers.append(worker)

            overloaded_workers.extend(service_overloaded_workers)
        return overloaded_workers

    def analyse_service_worker_overloaded(self, event_data):
        event_type = 'ServiceWorkerOverloadedPlanRequested'
        event_change_plan_data = None
        self.overloaded_workers = self.verify_service_workers_overloaded(event_data)

        if len(self.overloaded_workers) != 0:
            event_change_plan_data = self.build_change_plan_request_data(
                event_type=event_type, change_cause=event_data
            )
        return event_change_plan_data

    def _is_worker_idle(self, service_worker):
        queue_size = int(service_worker['queue_size'])
        return queue_size == 0

    def verify_service_worker_best_idle(self, service_workers):
        for service_type, service_type_dict in service_workers.items():
            idle_workers_keys = []
            for worker, worker_data in service_type_dict['workers'].items():
                if self._is_worker_idle(worker_data):
                    idle_workers_keys.append(worker)

            # if all workers of that type are idle than it doesn't matter
            if len(idle_workers_keys) != service_type_dict['total_number_workers']:
                for qos_policy in self.query_qos_policies.keys():
                    best_worker = self.best_workers_by_service_by_qos_policy.get(qos_policy, {}).get(service_type)
                    if best_worker and best_worker.get('stream_key') in idle_workers_keys:
                        return True
        return False

    def analyse_service_worker_best_idle(self, event_data):
        event_type = 'ServiceWorkerBestIdlePlanRequested'
        event_change_plan_data = None
        service_workers = event_data['service_workers']
        has_idle_best_worker = self.verify_service_worker_best_idle(service_workers)
        if has_idle_best_worker:
            event_change_plan_data = self.build_change_plan_request_data(
                event_type=event_type, change_cause=event_data
            )
            return event_change_plan_data

    def verify_unnecessary_load_shedding_for_dataflow(self, overloaded_workers_keys, dataflow):
        for worker_key_list in dataflow:
            worker_key = worker_key_list[0]
            if worker_key in overloaded_workers_keys:
                return False

        return True

    def filter_dataflow_choices_with_load_shedding(self, dataflow_choices):
        filtered = []
        if len(dataflow_choices) > 0 and len(dataflow_choices[0]) == 3:
            for choice in dataflow_choices:
                load_shedding = float(choice[0])
                if load_shedding > 0:
                    filtered.append[choice]

        return filtered

    def verify_unnecessary_load_shedding(self, event_data):
        if self.current_plan is not None:
            execution_plan_strategy = self.current_plan.get('execution_plan', {}).get('strategy', {})
            strategy_name = execution_plan_strategy.get('name', '')
            is_load_shedding_strategy = 'load_shedding' in strategy_name
            if is_load_shedding_strategy:
                # if there are no overloaded_workers, than no load shedding should exist
                if len(self.overloaded_workers) == 0:
                    return True

                dataflow_choices = execution_plan_strategy.get('dataflows', [])
                dataflow_choices_with_load_shedding = self.filter_dataflow_choices_with_load_shedding(dataflow_choices)
                overloaded_workers_keys = set([
                    w['stream_key'] for w in self.overloaded_workers])

                for choice in dataflow_choices_with_load_shedding:
                    dataflow = choice[2]
                    has_unnecessary_load_shedding = self.verify_unnecessary_load_shedding_for_dataflow(
                        overloaded_workers_keys, dataflow)
                    if has_unnecessary_load_shedding:
                        return True
        return False

    def analyse_unnecessary_load_shedding(self, event_data):
        event_type = 'UnnecessaryLoadSheddingPlanRequested'
        event_change_plan_data = None
        has_unnecessary_load_shedding = self.verify_unnecessary_load_shedding(event_data)
        if has_unnecessary_load_shedding:
            event_change_plan_data = self.build_change_plan_request_data(
                event_type=event_type, change_cause=event_data
            )
            return event_change_plan_data

    def process_service_workers_stream_monitored(self, event_data):
        service_worker_size_analysis = [
            self.analyse_service_worker_overloaded,
            self.analyse_service_worker_best_idle,
            # self.analyse_unnecessary_load_shedding,
        ]

        for analysis in service_worker_size_analysis:
            result = analysis(event_data=event_data)
            if result is not None:
                event_type = result['change']['type']
                self.publish_event_type_to_stream(event_type=event_type, new_event_data=result)
                break
        self.last_service_workers_monitoring = event_data

    def process_event_type(self, event_type, event_data, json_msg):
        if not super(AdaptationAnalyser, self).process_event_type(event_type, event_data, json_msg):
            return False

        if event_type == 'QueryCreated':
            self.process_query_created(event_data)
        elif event_type == 'ServiceWorkerAnnounced':
            self.process_service_worker_announced(event_data)
        elif event_type == 'ServiceWorkersStreamMonitored':
            self.process_service_workers_stream_monitored(event_data)

    def log_state(self):
        super(AdaptationAnalyser, self).log_state()
        self._log_dict('Recent Change Requests', self.recent_plan_change_requests_timestamps)
        self._log_dict('Best Workers by service by QOS policy', self.best_workers_by_service_by_qos_policy)

    def run(self):
        super(AdaptationAnalyser, self).run()
        self.cmd_thread = threading.Thread(target=self.run_forever, args=(self.process_cmd,))
        self.cmd_thread.start()
        self.cmd_thread.join()
