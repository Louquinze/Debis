from util.functions import get_user_properties, get_vertical_partitions
import warnings

warnings.filterwarnings("ignore")
partitions = get_vertical_partitions(["userId", "follows", "friendOf", "likes", "hasReview"])
for key in partitions:
    pa = partitions[key]
    print(f"\n\n\n{key}:\n{pa}\n\n\n\n\n")