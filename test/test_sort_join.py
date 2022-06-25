import os
import unittest
import os
from util.functions import base_sort_join, sort_join
# base_sort_join(build_r, probe_r, build_key, probe_key, keep_key=False)

class TestsortJoin(unittest.TestCase):
    def test_base_sort_join_1(self):
        build_r = [(i, i) for i in range(3)]
        probe_r = [(i, i) for i in range(2)]

        self.assertEqual([tup for tup in base_sort_join(build_r, probe_r, "object", "subject", keep_key=False)],
                         [(0, 0), (1, 1)])  # add assertion here

    def test_base_sort_join_2(self):
        build_r = [(i, i + 1) for i in range(3)]
        probe_r = [(i, i) for i in range(2)]
        self.assertEqual([tup for tup in base_sort_join(build_r, probe_r, "object", "subject", keep_key=True)],
                         [(0, 1, 1)])  # add assertion here


    def test_base_sort_join_3(self):
        print(os.getcwd())
        build_r = [(i, i + 1) for i in range(3)]
        probe_r = [(i, i) for i in range(2)]
        self.assertEqual([tup for tup in base_sort_join(build_r, probe_r, "subject", "object", keep_key=True)],
                         [(0, 0, 1), (1, 1, 2)])  # add assertion here



if __name__ == '__main__':
    unittest.main()