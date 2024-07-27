import math
from unittest import TestCase
from unittest.mock import MagicMock

from adaptation_analyser.uncertainty.ua_analysis import UAServiceAnalysis

class TestUAServiceAnalysis(TestCase):

    def setUp(self):
        self.parent_service = MagicMock()
        self.adaptation_delta = 10
        self.parent_service.adaptation_delta = self.adaptation_delta

        self.service_type = "SomeService"
        self.initialize_ua_analysis()
        self.worker_1 = {
            "throughput": 10,
            "queue_size": 0,
        }

        self.worker_2 = {
            "throughput": 50,
            "queue_size": 0,
        }

        self.worker_3 = {
            "throughput": 80.05,
            "queue_size": 0,
        }

        self.worker_4 = {
            "throughput": 100,
            "queue_size": 0,
        }

        self.workers_set_a = {
            'worker1': self.worker_1,
            'worker3': self.worker_3,
        }

        self.workers_set_b = {
            'worker1': self.worker_1,
            'worker2': self.worker_2,
            'worker3': self.worker_3,
            'worker4': self.worker_4,
        }

    def initialize_ua_analysis(self):
        self.ua_analysis = UAServiceAnalysis(
            self.parent_service, self.service_type)

    def test_setup_from_workers_corretly_sets_fuzzy_variables_when_workers_set_a_is_used(self):
        self.ua_analysis.setup_from_workers(self.workers_set_a)

        self.assertEqual(self.ua_analysis.sw_max_throughput, 80.05)
        self.assertEqual(self.ua_analysis.sw_max_cap, 800)
        self.assertEqual(self.ua_analysis.service_universe[0], 0)
        self.assertEqual(self.ua_analysis.service_universe[-1], 800)
        self.assertIsNotNone(self.ua_analysis.fis)
        self.assertIsNotNone(self.ua_analysis.fis)

    def test_setup_from_workers_corretly_sets_fuzzy_variables_when_workers_set_b_is_used(self):
        self.ua_analysis.setup_from_workers(self.workers_set_b)

        self.assertEqual(self.ua_analysis.sw_max_throughput, 100)
        self.assertEqual(self.ua_analysis.sw_max_cap, 1000)
        self.assertEqual(self.ua_analysis.service_universe[0], 0)
        self.assertEqual(self.ua_analysis.service_universe[-1], 1000)
        self.assertIsNotNone(self.ua_analysis.fis)
        self.assertIsNotNone(self.ua_analysis.fis)

    def test_setup_from_workers_corretly_sets_fuzzy_variables_when_changing_workers_set_a_to_b(self):
        self.ua_analysis.setup_from_workers(self.workers_set_a)
        self.ua_analysis.setup_from_workers(self.workers_set_b)

        self.assertEqual(self.ua_analysis.sw_max_throughput, 100)
        self.assertEqual(self.ua_analysis.sw_max_cap, 1000)
        self.assertEqual(self.ua_analysis.service_universe[0], 0)
        self.assertEqual(self.ua_analysis.service_universe[-1], 1000)
        self.assertIsNotNone(self.ua_analysis.fis)
        self.assertIsNotNone(self.ua_analysis.fis)

    #worker 1
    def test_calculate_worker_usage_returns_sound_result_when_queue_size_is_very_low_for_worker1(self):
        self.ua_analysis.setup_from_workers(self.workers_set_b)
        queue_size = 1
        max_capacity = math.floor(self.worker_1['throughput'] * self.adaptation_delta)
        usage = self.ua_analysis.calculate_worker_usage(queue_size=queue_size, max_capacity=max_capacity)
        crisp_usage_ref = queue_size / max_capacity * 100
        self.assertLessEqual(usage, 70)

    def test_calculate_worker_usage_returns_sound_result_when_queue_size_is_low_for_worker1(self):
        self.ua_analysis.setup_from_workers(self.workers_set_b)
        queue_size = 15
        max_capacity = math.floor(self.worker_1['throughput'] * self.adaptation_delta)
        usage = self.ua_analysis.calculate_worker_usage(queue_size=queue_size, max_capacity=max_capacity)
        crisp_usage_ref = queue_size / max_capacity * 100
        # print(crisp_usage_ref, usage)
        self.assertLessEqual(usage, 70)

    def test_calculate_worker_usage_returns_sound_result_when_queue_size_is_medium_high_for_worker1(self):
        self.ua_analysis.setup_from_workers(self.workers_set_b)
        queue_size = 65
        max_capacity = math.floor(self.worker_1['throughput'] * self.adaptation_delta)
        usage = self.ua_analysis.calculate_worker_usage(queue_size=queue_size, max_capacity=max_capacity)
        crisp_usage_ref = queue_size / max_capacity * 100
        # print(crisp_usage_ref, usage)
        self.assertGreaterEqual(usage, 65)
        self.assertLessEqual(usage, 80)

    def test_calculate_worker_usage_returns_sound_result_when_queue_size_is_very_high_for_worker1(self):
        self.ua_analysis.setup_from_workers(self.workers_set_b)
        queue_size = 190
        max_capacity = math.floor(self.worker_1['throughput'] * self.adaptation_delta)
        usage = self.ua_analysis.calculate_worker_usage(queue_size=queue_size, max_capacity=max_capacity)
        crisp_usage_ref = queue_size / max_capacity * 100
        # print(crisp_usage_ref, usage)
        self.assertGreaterEqual(usage, 70)
        self.assertLessEqual(usage, 90)


    #worker2
    # def test_calculate_worker_usage_returns_sound_result_when_queue_size_is_very_low_for_worker2(self):
    #     self.ua_analysis.setup_from_workers(self.workers_set_b)
    #     queue_size = 50
    #     max_capacity = math.floor(self.worker_2['throughput'] * self.adaptation_delta)
    #     usage = self.ua_analysis.calculate_worker_usage(queue_size=queue_size, max_capacity=max_capacity)
    #     crisp_usage_ref = queue_size / max_capacity * 100
    #     # print(crisp_usage_ref, usage)
    #     self.assertLessEqual(usage, 20)

    def test_calculate_worker_usage_returns_sound_result_when_queue_size_is_low_for_worker2(self):
        self.ua_analysis.setup_from_workers(self.workers_set_b)
        queue_size = 150
        max_capacity = math.floor(self.worker_2['throughput'] * self.adaptation_delta)
        usage = self.ua_analysis.calculate_worker_usage(queue_size=queue_size, max_capacity=max_capacity)
        crisp_usage_ref = queue_size / max_capacity * 100
        # print(crisp_usage_ref, usage)
        self.assertLessEqual(usage, 70)

    def test_calculate_worker_usage_returns_sound_result_when_queue_size_is_medium_high_for_worker2(self):
        self.ua_analysis.setup_from_workers(self.workers_set_b)
        queue_size = 325
        # queue_size = 355
        max_capacity = math.floor(self.worker_2['throughput'] * self.adaptation_delta)
        usage = self.ua_analysis.calculate_worker_usage(queue_size=queue_size, max_capacity=max_capacity)
        crisp_usage_ref = queue_size / max_capacity * 100
        # print(crisp_usage_ref, usage)
        self.assertGreaterEqual(usage, 65)
        self.assertLessEqual(usage, 80)

    def test_calculate_worker_usage_returns_sound_result_when_queue_size_is_high_for_worker2(self):
        self.ua_analysis.setup_from_workers(self.workers_set_b)
        queue_size = 485
        max_capacity = math.floor(self.worker_2['throughput'] * self.adaptation_delta)
        usage = self.ua_analysis.calculate_worker_usage(queue_size=queue_size, max_capacity=max_capacity)
        crisp_usage_ref = queue_size / max_capacity * 100
        # print(crisp_usage_ref, usage)
        self.assertGreaterEqual(usage, 70)
        self.assertLessEqual(usage, 90)

    def test_calculate_worker_usage_returns_sound_result_when_queue_size_is_very_high_for_worker2(self):
        self.ua_analysis.setup_from_workers(self.workers_set_b)
        queue_size = 500
        max_capacity = math.floor(self.worker_2['throughput'] * self.adaptation_delta)
        usage = self.ua_analysis.calculate_worker_usage(queue_size=queue_size, max_capacity=max_capacity)
        crisp_usage_ref = queue_size / max_capacity * 100
        # print(crisp_usage_ref, usage)
        self.assertGreaterEqual(usage, 85)

    #worker3
    # def test_calculate_worker_usage_returns_sound_result_when_queue_size_is_very_low_for_worker3(self):
    #     self.ua_analysis.setup_from_workers(self.workers_set_b)
    #     queue_size = 90
    #     max_capacity = math.floor(self.worker_3['throughput'] * self.adaptation_delta)
    #     usage = self.ua_analysis.calculate_worker_usage(queue_size=queue_size, max_capacity=max_capacity)
    #     crisp_usage_ref = queue_size / max_capacity * 100
    #     # print(crisp_usage_ref, usage)
    #     self.assertLessEqual(usage, 20)

    def test_calculate_worker_usage_returns_sound_result_when_queue_size_is_low_for_worker3(self):
        self.ua_analysis.setup_from_workers(self.workers_set_b)
        queue_size = 240
        max_capacity = math.floor(self.worker_3['throughput'] * self.adaptation_delta)
        usage = self.ua_analysis.calculate_worker_usage(queue_size=queue_size, max_capacity=max_capacity)
        crisp_usage_ref = queue_size / max_capacity * 100
        # print(crisp_usage_ref, usage)
        self.assertLessEqual(usage, 70)

    def test_calculate_worker_usage_returns_sound_result_when_queue_size_is_medium_high_for_worker3(self):
        self.ua_analysis.setup_from_workers(self.workers_set_b)
        queue_size = 520
        max_capacity = math.floor(self.worker_3['throughput'] * self.adaptation_delta)
        usage = self.ua_analysis.calculate_worker_usage(queue_size=queue_size, max_capacity=max_capacity)
        crisp_usage_ref = queue_size / max_capacity * 100
        # print(crisp_usage_ref, usage)
        self.assertGreaterEqual(usage, 65)
        self.assertLessEqual(usage, 80)

    def test_calculate_worker_usage_returns_sound_result_when_queue_size_is_high_for_worker3(self):
        self.ua_analysis.setup_from_workers(self.workers_set_b)
        queue_size = 750
        max_capacity = math.floor(self.worker_3['throughput'] * self.adaptation_delta)
        usage = self.ua_analysis.calculate_worker_usage(queue_size=queue_size, max_capacity=max_capacity)
        crisp_usage_ref = queue_size / max_capacity * 100
        # print(crisp_usage_ref, usage)
        self.assertGreaterEqual(usage, 70)
        self.assertLessEqual(usage, 90)

    def test_calculate_worker_usage_returns_sound_result_when_queue_size_is_very_high_for_worker3(self):
        self.ua_analysis.setup_from_workers(self.workers_set_b)
        queue_size = 800
        max_capacity = math.floor(self.worker_3['throughput'] * self.adaptation_delta)
        usage = self.ua_analysis.calculate_worker_usage(queue_size=queue_size, max_capacity=max_capacity)
        crisp_usage_ref = queue_size / max_capacity * 100
        # print(crisp_usage_ref, usage)
        self.assertGreaterEqual(usage, 80)


    #worker4
    # def test_calculate_worker_usage_returns_sound_result_when_queue_size_is_very_low_for_worker4(self):
    #     self.ua_analysis.setup_from_workers(self.workers_set_b)
    #     queue_size = 200
    #     max_capacity = math.floor(self.worker_4['throughput'] * self.adaptation_delta)
    #     usage = self.ua_analysis.calculate_worker_usage(queue_size=queue_size, max_capacity=max_capacity)
    #     crisp_usage_ref = queue_size / max_capacity * 100
    #     # print(crisp_usage_ref, usage)
    #     self.assertLessEqual(usage, 20)

    def test_calculate_worker_usage_returns_sound_result_when_queue_size_is_low_for_worker4(self):
        self.ua_analysis.setup_from_workers(self.workers_set_b)
        queue_size = 350
        max_capacity = math.floor(self.worker_4['throughput'] * self.adaptation_delta)
        usage = self.ua_analysis.calculate_worker_usage(queue_size=queue_size, max_capacity=max_capacity)
        crisp_usage_ref = queue_size / max_capacity * 100
        # print(crisp_usage_ref, usage)
        self.assertLessEqual(usage, 70)

    def test_calculate_worker_usage_returns_sound_result_when_queue_size_is_medium_high_for_worker4(self):
        self.ua_analysis.setup_from_workers(self.workers_set_b)
        queue_size = 675
        max_capacity = math.floor(self.worker_4['throughput'] * self.adaptation_delta)
        usage = self.ua_analysis.calculate_worker_usage(queue_size=queue_size, max_capacity=max_capacity)
        crisp_usage_ref = queue_size / max_capacity * 100
        # print(crisp_usage_ref, usage)
        self.assertGreaterEqual(usage, 65)
        self.assertLessEqual(usage, 80)

    def test_calculate_worker_usage_returns_sound_result_when_queue_size_is_high_for_worker4(self):
        self.ua_analysis.setup_from_workers(self.workers_set_b)
        queue_size = 850
        max_capacity = math.floor(self.worker_4['throughput'] * self.adaptation_delta)
        usage = self.ua_analysis.calculate_worker_usage(queue_size=queue_size, max_capacity=max_capacity)
        crisp_usage_ref = queue_size / max_capacity * 100
        # print(crisp_usage_ref, usage)
        self.assertGreaterEqual(usage, 70)
        self.assertLessEqual(usage, 90)

    def test_calculate_worker_usage_returns_sound_result_when_queue_size_is_very_high_for_worker4(self):
        self.ua_analysis.setup_from_workers(self.workers_set_b)
        queue_size = 1000
        max_capacity = math.floor(self.worker_4['throughput'] * self.adaptation_delta)
        usage = self.ua_analysis.calculate_worker_usage(queue_size=queue_size, max_capacity=max_capacity)
        crisp_usage_ref = queue_size / max_capacity * 100
        # print(crisp_usage_ref, usage)
        self.assertGreaterEqual(usage, 85)