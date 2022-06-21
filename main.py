from util.functions import get_user_properties, get_vertical_partitions, base_hash_join
import warnings

warnings.filterwarnings("ignore")
partitions = get_vertical_partitions(["likes", "hasReview"])
print(partitions)
join = base_hash_join(partitions["likes"], partitions["hasReview"], "object", "subject")

print(join)
