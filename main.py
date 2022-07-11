import os
import argparse

from util.functions import get_user_properties, get_vertical_partitions, base_hash_join, hash_join, base_sort_join, \
    sort_join

import warnings
import time
import datetime

warnings.filterwarnings("ignore")

parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('type', type=str, default="hash")  # sort
parser.add_argument('dataset', type=str, default="small")  # huge

if __name__ == '__main__':
    args = parser.parse_args()
    os.mkdir("tmp/sort")
    partitions = {}
    for key in ["follows", "friendOf", "likes", "hasReview"]:
        partitions[key] = get_vertical_partitions(key
                                                  # , "watdiv.10M/watdiv.10M.nt"
                                                  )
    # _______________________________________________________________________________________________________________________
    start = time.time()
    if args.dataset == "huge":
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
    # _______________________________________________________________________________________________________________________
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
        "memory_limit": 0.05
    }
    if args.type == "sort":
        join = sort_join(**kwargs)  # buffer.append((join, hash_table))
    else:
        join = hash_join(**kwargs)

    end = time.time()
    delta = end - start
    sec = delta % 60
    delta = delta // 60
    min = delta % 60
    delta = delta // 60
    print(f"finisehd in {delta}:{min}:{sec}")
    with open("log.text", "a") as f:
        f.write(f"args_{args.type}_{args.dataset}: {delta}:{min}:{sec}\n")

    with open("result_sort_join.csv", "w") as f:
        f.write("follows.subject,follows.object,friendOf.object,likes.object,hasReview.object\n")
        for elem in join:
            f.write(f"{elem[0]},{elem[1]},{elem[2]},{elem[3]},{elem[4]}\n")
