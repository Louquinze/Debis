import os
import argparse

from util.functions import get_user_properties, get_vertical_partitions, base_hash_join, hash_join, base_sort_join, \
    sort_join, sort_parallel_join

import warnings
import time
import datetime

warnings.filterwarnings("ignore")

parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('--type', type=str, default="hash")  # sort
parser.add_argument('--dataset', type=str, default="small")  # huge

if __name__ == '__main__':
    args = parser.parse_args()
    if args.type in ["sort", "sort_parallel"]:
        os.mkdir("tmp/sort")
    partitions = {}
    for key in ["follows", "friendOf", "likes", "hasReview"]:
        if args.dataset == "huge":
            partitions[key] = get_vertical_partitions(key, "watdiv.10M/watdiv.10M.nt")
        else:
            partitions[key] = get_vertical_partitions(key)
    # _______________________________________________________________________________________________________________________
    start = time.time()
    if args.type in ["sort", "sort_parallel"]:
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
                if (idx + 1) % 1e7 == 0:
                    count += 1

            count = 0
            partitions[key].sort(key=lambda tup: tup[-1])
            for idx, elem in enumerate(partitions[key]):
                with open(f"tmp/sort/{key}/object/{count}.csv", "a") as f:
                    f.write(" ".join(str(x) for x in elem))
                    f.write("\n")
                if (idx + 1) % 1e7 == 0:
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
        "memory_limit": 2  # 10 GiB RAM
    }
    if args.type == "sort":
        join = sort_join(**kwargs)  # buffer.append((join, hash_table))
    elif args.type == "sort_parallel":
        join = sort_parallel_join(**kwargs)
    else:
        join = hash_join(**kwargs)

    last_idx = 0
    if args.type in ["sort", "sort_parallel"]:
        for file in os.listdir("tmp/sort/3/subject"):
            os.system(f"mv tmp/sort/3/subject/{f} result_{args.type}_join_{args.dataset}.csv")
    else:
        with open(f"result_{args.type}_join_{args.dataset}.csv", "w") as f:
            f.write("follows.subject,follows.object,friendOf.object,likes.object,hasReview.object\n")
            for elem in join:
                f.write(f"{elem[0]},{elem[1]},{elem[2]},{elem[3]},{elem[4]}\n")

    last_idx = 0
    with open(f"result_{args.type}_join_{args.dataset}.csv", "r") as f:
        for _ in f:
            last_idx += 1

    end = time.time()
    delta = end - start
    sec = delta % 60
    delta = delta // 60
    min = delta % 60
    delta = delta // 60
    print(f"finisehd in {delta}:{min}:{sec}")
    with open(f"result_{args.type}_join_{args.dataset}.txt", "w") as f:
        f.write(f"args_{args.type}_{args.dataset}: {delta}:{min}:{sec}\n"
                f"idx: {last_idx}\n\n")
