from util.functions import get_user_properties, get_vertical_partitions, base_hash_join, hash_join
import warnings

warnings.filterwarnings("ignore")
partitions = get_vertical_partitions(["follows", "friendOf", "likes", "hasReview"])
# print(partitions)
join = base_hash_join(partitions["likes"], partitions["hasReview"], "object", "subject")
# print(join)

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
join = hash_join(**kwargs)
print(join)

