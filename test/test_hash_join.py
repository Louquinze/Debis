import unittest
from util.functions import base_hash_join, hash_join
# base_hash_join(build_r, probe_r, build_key, probe_key, keep_key=False)

class TestHashJoin(unittest.TestCase):
    def test_base_hash_join(self):
        build_r = [(i, i) for i in range(3)]
        probe_r = [(i, i) for i in range(2)]
        self.assertEqual([tup for tup in base_hash_join(build_r, probe_r, "object", "subject", keep_key=False)],
                         [(0, 0), (1, 1)])  # add assertion here

        build_r = [(i, i+1) for i in range(3)]
        probe_r = [(i, i) for i in range(2)]
        self.assertEqual([tup for tup in base_hash_join(build_r, probe_r, "object", "subject", keep_key=True)],
                         [(0, 1, 1)])  # add assertion here

        build_r = [(i, i + 1) for i in range(3)]
        probe_r = [(i, i) for i in range(2)]
        for tup in base_hash_join(build_r, probe_r, "subject", "object", keep_key=True):
            print(tup)
        self.assertEqual([tup for tup in base_hash_join(build_r, probe_r, "subject", "object", keep_key=True)],
                         [(0, 0, 1), (1, 1, 2)])  # add assertion here

    def test_hash_join(self):
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

        # build_r, probe_r, build_key, probe_key
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
        join = hash_join(**kwargs)  # buffer.append((join, hash_table))
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

    def test_hash_join_big(self):
        partitions = {}
        partitions[0] = [(1, 1) for _ in range(int(1e7))]
        partitions[1] = [(1, 1) for _ in range(int(1e7/2))] + [(2, 2) for _ in range(int(1e7/2))]
        partitions[2] = [(1, 1) for _ in range(int(1e7/4))]

        kwargs = {
            "build_r_1": partitions[0],
            "probe_r_1": partitions[1],
            "build_key_1": "object",
            "probe_key_1": "subject",
            "build_r_2": partitions[1],
            "probe_r_2": partitions[2],
            "build_key_2": "object",
            "probe_key_2": "subject",
            "num_joins": 2

        }

        join = hash_join(**kwargs)  # buffer.append((join, hash_table))
        c = 0
        for elem in join:
        #     self.assertIn(elem, res)
            if c % 1e6 == 0:
                print(elem)
            c += 1
        self.assertEqual(c, int(1e7/4))


if __name__ == '__main__':
    unittest.main()
