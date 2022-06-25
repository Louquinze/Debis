import unittest
import os
from util.functions import base_sort_join, sort_join
import pickle


# base_sort_join(build_r, probe_r, build_key, probe_key, keep_key=False)

class TestsortJoin(unittest.TestCase):
    def test_base_sort_join_1(self):
        os.mkdir("tmp/b_sort_test_root")
        os.mkdir("tmp/p_sort_test_root")
        os.mkdir("tmp/sort")

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
            path = base_sort_join(build_r, probe_r, "object", "subject", "test", keep_key=False)
            with open(f"{path}/0.pkl", "rb") as f:
                res = pickle.load(f)
            self.assertEqual(res, [(1, 1), (2, 2), (3, 3)])  # add assertion here
        finally:
            for file in os.listdir(build_r):
                os.remove(f"{build_r}{file}")
            os.rmdir(build_r)

            for file in os.listdir(probe_r):
                os.remove(f"{probe_r}{file}")
            os.rmdir(probe_r)

            for directory in os.listdir("tmp/sort"):
                for file in os.listdir(f"tmp/sort/{directory}"):
                    os.remove(f"tmp/sort/{directory}/{file}")
                os.rmdir(f"tmp/sort/{directory}")
            os.rmdir("tmp/sort")

    def test_sort_join(self):
        os.mkdir("tmp/sort")

        partitions = dict()
        partitions["follows"] = [
            ("Hannah", "Lukas"),
            ("Hannah", "Adina"),
            ("Lukas", "Hannah"),
            ("Adina", "Lukas"),
        ]

        partitions["friendOf"] = [
            ("Hannah", "Lukas"),
            ("Adina", "Lukas"),
        ]

        partitions["likes"] = [
            ("Lukas", "P0"),
            ("Lukas", "P1"),
            ("Hannah", "P1"),
            ("Hannah", "P2"),
            ("Adina", "P1"),
            ("Adina", "P0"),
        ]

        partitions["hasReview"] = [
            ("P0", "R0"),
            ("P0", "R1"),
            ("P2", "R2"),
        ]

        # write sorted list to drive
        for key in partitions:
            os.mkdir(f"tmp/sort/{key}")
            os.mkdir(f"tmp/sort/{key}/subject")
            os.mkdir(f"tmp/sort/{key}/object")

            for idx, elem in enumerate(sorted(partitions[key], key=lambda tup: tup[0])):
                with open(f"tmp/sort/{key}/subject/{idx}.pkl", "wb") as f:
                    pickle.dump([elem], f)
            for idx, elem in enumerate(sorted(partitions[key], key=lambda tup: tup[1])):
                with open(f"tmp/sort/{key}/object/{idx}.pkl", "wb") as f:
                    pickle.dump([elem], f)
            partitions[key] = {"subject": f"tmp/sort/{key}/subject",
                               "object": f"tmp/sort/{key}/object"}

        kwargs = {
            "build_r_1": partitions["follows"],
            "probe_r_1": partitions["friendOf"],
            "build_key_1": "object",
            "probe_key_1": "subject",
            "build_r_2": partitions["friendOf"],
            "probe_r_2": partitions["likes"],
            "build_key_2": "object",
            "probe_key_2": "subject",
            "build_r_3": partitions["likes"],
            "probe_r_3": partitions["hasReview"],
            "build_key_3": "object",
            "probe_key_3": "subject",
            "num_joins": 3

        }
        join = sort_join(**kwargs)  # buffer.append((join, sort_table))
        res = [
            ("Lukas", "Hannah", "Lukas", "P0", "R0"),
            ("Hannah", "Adina", "Lukas", "P0", "R0"),
            ("Lukas", "Hannah", "Lukas", "P0", "R1"),
            ("Hannah", "Adina", "Lukas", "P0", "R1"),
        ]
        c = 0
        for elem in join:
            self.assertIn(elem, res)
            c += 1
        self.assertEqual(c, len(res))

        for folder in os.listdir("tmp"):
            if "sort" in folder:
                for elem in os.listdir(f"tmp/{folder}"):
                    os.remove(f"tmp/{folder}/{elem}")
                os.rmdir(f"tmp/{folder}")

        for elem in os.listdir(f"tmp/partitions"):
            os.remove(f"tmp/partitions/{elem}")


if __name__ == '__main__':
    unittest.main()
