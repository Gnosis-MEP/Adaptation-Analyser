import math

import numpy as np
import matplotlib.pyplot as plt
import skfuzzy as fuzz
from skfuzzy import control as ctrl

from adaptation_analyser.conf import MF_LABELS


class UAServiceAnalysis(object):
    def __init__(self, parent_service, service_type):
        self.parent_service = parent_service
        self.service_type = service_type
        self.fis = None
        self.sim = None
        self.sw_max_throughput = 0.0
        self.sw_max_cap = 0
        self.service_universe = None
        self.usage_universe = np.arange(0, 100 + 1, 1)
        self.has_changed = False

    def setup_from_workers(self, workers):
        self.has_changed = False
        for worker, worker_data in workers.items():
            throughput = float(worker_data.get('throughput', 0.0))
            if throughput > self.sw_max_throughput:
                self.sw_max_throughput = throughput
                self.has_changed = True

        if self.has_changed:
            self.sw_max_cap = math.floor(self.sw_max_throughput * self.parent_service.adaptation_delta)
            self.service_universe = np.arange(0, self.sw_max_cap + 1, 1)
            del self.fis
            del self.sim
            self.build_fis()
            self.build_sim()
            self.has_changed = False

    def build_fis(self):
        adp_max_cap = ctrl.Antecedent(self.service_universe, 'max_capacity')
        adp_max_cap.automf(names=MF_LABELS)
        # adp_max_cap.view()
        # plt.savefig('adp_max_cap.png')

        queue_size = ctrl.Antecedent(self.service_universe, 'queue_size')
        queue_size.automf(names=MF_LABELS)
        # queue_size.view()
        # plt.savefig('queue_size.png')

        usage = ctrl.Consequent(self.usage_universe, 'usage')
        usage.automf(names=MF_LABELS)
        # usage.view()
        # plt.savefig('usage.png')
        self.rules = []

        # rules for queue_size very_low
        self.rules.append(
            ctrl.Rule(queue_size['very_low'], usage['very_low'])
        )
        # self.rules.append(
        #     ctrl.Rule(adp_max_cap['very_low'] & queue_size['very_low'], usage['high'])
        # )
        # self.rules.append(
        #     ctrl.Rule((~adp_max_cap['very_low']) & queue_size['very_low'], usage['very_low'])
        # )

        # rules for queue_size low
        self.rules.append(
            ctrl.Rule(adp_max_cap['very_low'] & queue_size['low'], usage['high'])
        )
        self.rules.append(
            ctrl.Rule(adp_max_cap['low'] & queue_size['low'], usage['medium'])
        )
        self.rules.append(
            ctrl.Rule((adp_max_cap['medium'] | adp_max_cap['high']) & queue_size['low'], usage['low'])
        )
        self.rules.append(
            ctrl.Rule(adp_max_cap['very_high'] & queue_size['low'], usage['very_low'])
        )
        # self.rules.append(
        #     ctrl.Rule(adp_max_cap['very_low'] & queue_size['low'], usage['very_high'])
        # )
        # self.rules.append(
        #     ctrl.Rule(adp_max_cap['low'] & queue_size['low'], usage['high'])
        # )
        # self.rules.append(
        #     ctrl.Rule((adp_max_cap['medium'] | adp_max_cap['high']) & queue_size['low'], usage['low'])
        # )
        # self.rules.append(
        #     ctrl.Rule(adp_max_cap['very_high'] & queue_size['low'], usage['very_low'])
        # )

        # rules for queue_size medium
        self.rules.append(
            ctrl.Rule(adp_max_cap['very_low'] & queue_size['medium'], usage['very_high'])
        )
        self.rules.append(
            ctrl.Rule(adp_max_cap['low'] & queue_size['medium'], usage['high'])
        )
        self.rules.append(
            ctrl.Rule(adp_max_cap['medium'] & queue_size['medium'], usage['high'])
        )
        self.rules.append(
            ctrl.Rule(adp_max_cap['high'] & queue_size['medium'], usage['medium'])
        )
        self.rules.append(
            ctrl.Rule(adp_max_cap['very_high'] & queue_size['medium'], usage['low'])
        )

        # rules for queue_size high
        self.rules.append(
            ctrl.Rule(adp_max_cap['very_low'] & queue_size['high'], usage['very_high'])
        )
        self.rules.append(
            ctrl.Rule(adp_max_cap['low'] & queue_size['high'], usage['very_high'])
        )
        self.rules.append(
            ctrl.Rule(adp_max_cap['medium'] & queue_size['high'], usage['very_high'])
        )
        self.rules.append(
            ctrl.Rule(adp_max_cap['high'] & queue_size['high'], usage['high'])
        )
        self.rules.append(
            ctrl.Rule(adp_max_cap['very_high'] & queue_size['high'], usage['medium'])
        )

        # rules for queue_size very_high
        self.rules.append(
            ctrl.Rule(adp_max_cap['very_low'] & queue_size['very_high'], usage['very_high'])
        )
        self.rules.append(
            ctrl.Rule(adp_max_cap['low'] & queue_size['very_high'], usage['very_high'])
        )
        self.rules.append(
            ctrl.Rule(adp_max_cap['medium'] & queue_size['very_high'], usage['very_high'])
        )
        self.rules.append(
            ctrl.Rule(adp_max_cap['high'] & queue_size['very_high'], usage['very_high'])
        )
        self.rules.append(
            ctrl.Rule(adp_max_cap['very_high'] & queue_size['very_high'], usage['high'])
        )

        self.fis = ctrl.ControlSystem(self.rules)

    def build_sim(self):
        self.sim = ctrl.ControlSystemSimulation(self.fis)

    def calculate_worker_usage(self, queue_size, max_capacity):
        queue_size_ceil = min(queue_size, self.sw_max_cap)
        self.sim.input['max_capacity'] = max_capacity
        self.sim.input['queue_size'] = queue_size_ceil
        self.sim.compute()
        return self.sim.output['usage']
