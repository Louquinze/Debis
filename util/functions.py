import re
import sys
import os
import pickle


def get_user_properties():
    properites = set()

    file1 = open('100k.txt', 'r')
    Lines = file1.readlines()
    for line in Lines:
        text = line.strip()
        rep = {"wsdbm:": "",
               "sorg:": "",
               "gr:": "",
               "foaf:": "",
               "gn:": "",
               "rdf:": "",
               '"': "",
               ".": "",
               "\t": " ",
               "rev:": "",
               "dc:": ""
               }  # define desired replacements here

        # use these three lines to do the replacement
        rep = dict((re.escape(k), v) for k, v in rep.items())
        pattern = re.compile("|".join(rep.keys()))
        text = pattern.sub(lambda m: rep[re.escape(m.group(0))], text)

        content = text.split(" ", 2)
        if "User" in content[0] and content[1] not in properites:
            properites.add(content[1])

    return properites


def get_vertical_partitions(key, filename='100k.txt'):
    # edges: follows, friendOf, likes, hasReview
    # nodes: User, Review

    file1 = open(filename, 'r')
    Lines = file1.readlines()
    for line in Lines:
        text = line.strip()
        if filename == '100k.txt':
            rep = {"wsdbm:": "",
                   "sorg:": "",
                   "gr:": "",
                   "foaf:": "",
                   "gn:": "",
                   "rdf:": "",
                   '"': "",
                   ".": "",
                   "\t": " ",
                   "rev:": "",
                   "dc:": ""}
        else:
            rep = {
                '"': "",
                ".": "",
                "\t": " ",
                ">": "",
                "<": "",
                "http://": ""
            }  # define desired replacements here

        # use these three lines to do the replacement
        rep = dict((re.escape(k), v) for k, v in rep.items())
        pattern = re.compile("|".join(rep.keys()))
        for r in ["dbuwaterlooca/~galuc/wsdbm/", "schemaorg/", "purlorg/goodrelations/", "/~galuc/wsdbm/",
                  "wwwgeonamesorg/ontology#", "purlorg/stuff/rev#", "ogpme/ns#", "wwww3org/1999/02/22-rdf-syntax-ns#",
                  ]:
            text = pattern.sub(lambda m: rep[re.escape(m.group(0))], text).replace(r, "")

        content = text.split(" ", 2)
        # Todo Map subject and object to numbers, maybe possible in same loop
        if content[1] == key:  # this is why big is not build
            yield content[0].replace(" ", ""), content[2].replace(" ", "")


def base_hash_join(build_r, probe_r, build_key, probe_key, keep_key=False, step=0, memory_limit=2):
    """
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
    """
    # build_r don't need to have only 2 values ? for easy chain
    hash_table_allc = {1: False}  # maps a hash table to if its full or not
    current_table_idx = 1
    hash_table = {}

    os.mkdir(f"tmp/hash_{step}")
    for build_i in build_r:
        if build_key == "subject":
            subject, object = build_i[0], build_i[1:]
            if subject not in hash_table:
                hash_table[subject] = set([object])  # create a bucket storing same hash
            else:
                hash_table[subject].add(object)
        elif build_key == "object":
            subject, object = build_i[:len(build_i) - 1], build_i[-1]
            if object not in hash_table:
                hash_table[object] = set([subject])
            else:
                hash_table[object].add(subject)
        # save hash_table to disk if it reaches a certain size
        if sys.getsizeof(hash_table) / 1e6 > memory_limit:  # 2 GiB
            # store to file
            with open(f"tmp/hash_{step}/hash_table_{current_table_idx}.pkl", "wb") as f:
                pickle.dump(hash_table, f)
            hash_table_allc[current_table_idx] = True

            # load new not full
            all_files_full = True
            for key, value in hash_table_allc.items():
                if value is False:
                    # load hash map
                    with open(f"tmp/hash_{step}/hash_table_{key}.pkl", "rb") as f:
                        hash_table = pickle.load(f)
                    current_table_idx = key
                    all_files_full = False
                    break

            if all_files_full:
                # create new
                hash_table_allc[max(hash_table_allc.keys()) + 1] = False
                current_table_idx = max(hash_table_allc.keys())
                hash_table = dict()

    with open(f"tmp/hash_{step}/hash_table_{current_table_idx}.pkl", "wb") as f:  # save last state
        pickle.dump(hash_table, f)

    for hash_table_path in os.listdir(f"tmp/hash_{step}"):
        with open(f"tmp/hash_{step}/{hash_table_path}", "rb") as f:
            hash_table = pickle.load(f)
        for probe_i in probe_r:
            subject, object = probe_i[-2], probe_i[-1]
            if probe_key == "subject":
                if subject in hash_table:
                    if keep_key:
                        for i in hash_table[subject]:
                            yield (*i, subject, object)
                    else:
                        for i in hash_table[subject]:
                            yield (*i, object)
            elif probe_key == "object":
                if object in hash_table:
                    if keep_key:
                        for i in hash_table[object]:
                            yield (subject, object, *i)
                    else:
                        for i in hash_table[object]:
                            yield (subject, *i)
        for file in os.listdir(f"tmp/hash_{step}"):
            os.remove(f"tmp/hash_{step}/{file}")
        os.rmdir(f"tmp/hash_{step}")


def hash_join(**kwargs):
    """
    use concatenation of base_hash_join to join multiple relations (subject, object)
    --> how to define this ? --> work with args

    def func_kwargs(**kwargs):
    print('kwargs: ', kwargs)
    print('type: ', type(kwargs))

    func_kwargs(key1=1, key2=2, key3=3)
    # kwargs:  {'key1': 1, 'key2': 2, 'key3': 3}
    # type:  <class 'dict'>

    --> indice build_r, probe_r, build_key, probe_key
    kwargs = {
        "build_r_init": partitions["follows"],
        "probe_r_1": partitions["friendOf"],
        "build_key_1": "object",
        "probe_key_1": "subject",
        "probe_r_2": partitions["likes"],
        "build_key_2": "object",
        "probe_key_2": "subject",
        "probe_r_3": partitions["hasReview"],
        "build_key_3": "object",
        "probe_key_3": "subject",
    }
    """
    # initial join
    buffer = []
    # join = base_hash_join(kwargs[f"build_r_1"], kwargs[f"probe_r_1"], kwargs[f"build_key_1"],
    #                       kwargs[f"probe_key_1"], keep_key=True)
    # buffer.append((join))
    keep_key = True
    for i in range(1, kwargs["num_joins"] + 1):
        if i == 1:
            join = base_hash_join(kwargs[f"build_r_{i}"], kwargs[f"probe_r_{i}"], kwargs[f"build_key_{i}"],
                                  kwargs[f"probe_key_{i}"], keep_key=keep_key, step=i)
        else:
            join = base_hash_join(last_join, kwargs[f"probe_r_{i}"], kwargs[f"build_key_{i}"], kwargs[f"probe_key_{i}"],
                                  keep_key=keep_key, step=i)
            # use base_hash_join again with abitary num of features but only 1 key
        last_join = join

    # todo how to implement the conjuctions
    return join


def base_parallel_sort_join(build_r, probe_r, build_key, probe_key, keep_key=False):
    if build_key == "subject":
        build_r.sort(key=lambda tup: tup[0])  # inplace
    elif build_key == "object":
        build_r.sort(key=lambda tup: tup[1])

    if probe_key == "subject":
        probe_r.sort(key=lambda tup: tup[0])
    elif probe_key == "object":
        probe_r.sort(key=lambda tup: tup[1])

    join = []
    for build_i in build_r:
        for probe_i in probe_r:
            if build_key == "subject" and probe_key == "subject":
                if build_i[0] == probe_i[0]:
                    if keep_key:
                        join.append((build_i[0], build_i[1], probe_i[1]))
                    else:
                        join.append((build_i[1], build_i[1]))
            elif build_key == "object" and probe_key == "subject":
                if build_i[1] == probe_i[0]:
                    if keep_key:
                        join.append((build_i[0], build_i[1], probe_i[1]))
                    else:
                        join.append((build_i[0], build_i[1]))
            elif build_key == "subject" and probe_key == "object":
                if build_i[0] == probe_i[1]:
                    if keep_key:
                        join.append((build_i[0], build_i[1], probe_i[0]))
                    else:
                        join.append((build_i[1], build_i[0]))
            elif build_key == "object" and probe_key == "object":
                if build_i[1] == probe_i[1]:
                    if keep_key:
                        join.append((build_i[0], build_i[1], probe_i[0]))
                    else:
                        join.append((build_i[0], build_i[0]))
    return join


def parallel_sort_join(**kwargs):
    """
    use concatenation of base_hash_join to join multiple relations (subject, object)
    --> how to define this ? --> work with args

    def func_kwargs(**kwargs):
    print('kwargs: ', kwargs)
    print('type: ', type(kwargs))

    func_kwargs(key1=1, key2=2, key3=3)
    # kwargs:  {'key1': 1, 'key2': 2, 'key3': 3}
    # type:  <class 'dict'>

    --> indice build_r, probe_r, build_key, probe_key
    kwargs = {
        "build_r_init": partitions["follows"],
        "probe_r_1": partitions["friendOf"],
        "build_key_1": "object",
        "probe_key_1": "subject",
        "probe_r_2": partitions["likes"],
        "build_key_2": "object",
        "probe_key_2": "subject",
        "probe_r_3": partitions["hasReview"],
        "build_key_3": "object",
        "probe_key_3": "subject",
    }
    """
    # initial join
    buffer = []
    # join = base_hash_join(kwargs[f"build_r_1"], kwargs[f"probe_r_1"], kwargs[f"build_key_1"],
    #                       kwargs[f"probe_key_1"], keep_key=True)
    # buffer.append((join))
    keep_key = True
    for i in range(1, kwargs["num_joins"] + 1):
        join = base_hash_join(kwargs[f"build_r_{i}"], kwargs[f"probe_r_{i}"], kwargs[f"build_key_{i}"],
                              kwargs[f"probe_key_{i}"], keep_key=keep_key)
        if i > 1:
            join = base_hash_join(last_join, join, "object", "subject", keep_key=keep_key)
            # use base_hash_join again with abitary num of features but only 1 key
        last_join = join

    # todo how to implement the conjuctions
    return join
