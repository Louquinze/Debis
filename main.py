from util.functions import get_user_properties, get_vertical_partitions, base_hash_join, hash_join, base_parallel_sort_join
import warnings

warnings.filterwarnings("ignore")

partitions = {}
for key in ["follows", "friendOf", "likes", "hasReview"]:
    partitions[key] = get_vertical_partitions(key)
    for i in partitions[key]:
        print(i)
        break

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
# with open("result_hash_join.csv", "w") as f:
#    f.write("follows.subject,follows.object,friendOf.object,likes.object,hasReview.object\n")
#     for elem in join:
#         f.write(f"{elem[0]},{elem[1]},{elem[2]},{elem[3]},{elem[4]}\n")

c = 0
for elem in join:
    if c % 1e6 == 0:
        print(f"{elem[0]}, {elem[1]}, {elem[2]}, {elem[3]}, {elem[4]}")
    c += 1
print(c)