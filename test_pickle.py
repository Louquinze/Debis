import pickle
import os

hash_table = {
    "A": set([i for i in range(10)]),
    "B": set([i for i in range(0, 20, 2)])
}
print(hash_table)

with open("test.pkl", "wb") as f:
    pickle.dump(hash_table, f)

del hash_table

with open("test.pkl", "rb") as f:
    hash_table = pickle.load(f)

print(hash_table)