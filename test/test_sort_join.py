import unittest
import os
from util.functions import base_sort_join, sort_join
import pickle
# base_sort_join(build_r, probe_r, build_key, probe_key, keep_key=False)

class TestsortJoin(unittest.TestCase):
    def test_base_sort_join_1(self):
        os.mkdir("tmp/b_sort_test_root")
        os.mkdir("tmp/p_sort_test_root")

        build_r = "tmp/b_sort_test_root/"
        probe_r = "tmp/p_sort_test_root/"

        with open(f"{build_r}1.pkl", "wb") as f:
            pickle.dump([(1, 1), (2, 2)], f)
        with open(f"{build_r}2.pkl", "wb") as f:
            pickle.dump([(3, 3), (4, 4)], f)

        with open(f"{probe_r}1.pkl", "wb") as f:
            pickle.dump([(0, 0), (1, 1)], f)
        with open(f"{probe_r}2.pkl", "wb") as f:
            pickle.dump([(2, 2), (3, 3)], f)

        try:
            self.assertEqual([tup for tup in base_sort_join(build_r, probe_r, "object", "subject", keep_key=False)],
                             [(1, 1), (2, 2), (3, 3)])  # add assertion here
        finally:
            for file in os.listdir(build_r):
                os.remove(f"{build_r}{file}")
            os.rmdir(build_r)

            for file in os.listdir(probe_r):
                os.remove(f"{probe_r}{file}")
            os.rmdir(probe_r)

    def test_base_sort_join_2(self):
        os.mkdir("tmp/b_sort_test_root")
        os.mkdir("tmp/p_sort_test_root")

        build_r = "tmp/b_sort_test_root/"
        probe_r = "tmp/p_sort_test_root/"

        with open(f"{build_r}1.pkl", "wb") as f:
            pickle.dump([[(i, i + 1) for i in range(3)][0]], f)
        with open(f"{build_r}2.pkl", "wb") as f:
            pickle.dump([(i, i + 1) for i in range(3)][1:], f)

        with open(f"{probe_r}1.pkl", "wb") as f:
            pickle.dump([(0, 0)], f)
        with open(f"{probe_r}2.pkl", "wb") as f:
            pickle.dump([(1, 1)], f)

        try:
            self.assertEqual([tup for tup in base_sort_join(build_r, probe_r, "object", "subject", keep_key=True)],
                             [(0, 1, 1)])  # add assertion here
        finally:
            for file in os.listdir(build_r):
                os.remove(f"{build_r}{file}")
            os.rmdir(build_r)

            for file in os.listdir(probe_r):
                os.remove(f"{probe_r}{file}")
            os.rmdir(probe_r)



if __name__ == '__main__':
    unittest.main()
