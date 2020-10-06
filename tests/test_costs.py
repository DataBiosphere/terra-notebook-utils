#!/usr/bin/env python
import os
import sys
import unittest

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from tests import config
from terra_notebook_utils import costs


class TestTerraNotebookUtilsCosts(unittest.TestCase):
    def test_costs_estimate_n1(self):
        tests = [(0.00043, True), (0.00203, False)]
        for cost, preemptible in tests:
            with self.subTest(preemptible=preemptible):
                self.assertEqual(cost, round(costs.GCPCustomN1Cost.estimate(3, 5, 7, preemptible), 5))

if __name__ == '__main__':
    unittest.main()
