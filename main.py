import os

from util.functions import get_user_properties, get_vertical_partitions, base_hash_join, hash_join, base_sort_join, \
    sort_join

import warnings

warnings.filterwarnings("ignore")

os.mkdir("tmp/sort")
partitions = {}
for key in ["follows", "friendOf", "likes", "hasReview"]:
    partitions[key] = get_vertical_partitions(key
                                              # , "watdiv.10M/watdiv.10M.nt"
                                              )
#_______________________________________________________________________________________________________________________
for key in partitions:
    os.mkdir(f"tmp/sort/{key}")
    os.mkdir(f"tmp/sort/{key}/subject")
    os.mkdir(f"tmp/sort/{key}/object")

    count = 0
    partitions[key] = list(partitions[key])
    partitions[key].sort(key=lambda tup: tup[0])
    for idx, elem in enumerate(partitions[key]):
        with open(f"tmp/sort/{key}/subject/{count}.csv", "a") as f:
            f.write(" ".join(str(x) for x in elem))
            f.write("\n")
        if idx % 1e6 == 0:
            count += 1

    count = 0
    partitions[key].sort(key=lambda tup: tup[-1])
    for idx, elem in enumerate(partitions[key]):
        with open(f"tmp/sort/{key}/object/{count}.csv", "a") as f:
            f.write(" ".join(str(x) for x in elem))
            f.write("\n")
        if idx % 1e6 == 0:
            count += 1

    partitions[key] = {"subject": f"tmp/sort/{key}/subject",
                       "object": f"tmp/sort/{key}/object"}
#_______________________________________________________________________________________________________________________
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
    "num_joins": 3,
    "memory_limit": 0.5
}
join = sort_join(**kwargs)  # buffer.append((join, hash_table))
# with open("result_hash_join.csv", "w") as f:
#    f.write("follows.subject,follows.object,friendOf.object,likes.object,hasReview.object\n")
#     for elem in join:
#         f.write(f"{elem[0]},{elem[1]},{elem[2]},{elem[3]},{elem[4]}\n")

c = 0
for elem in join:
    if c % 1e6 == 0:
        print(elem)
    c += 1
print(c)
