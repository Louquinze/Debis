import re
import sys
import os
import pickle
from heapq import merge


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
        # for file in os.listdir(f"tmp/hash_{step}"):
        #     os.remove(f"tmp/hash_{step}/{file}")
        # os.rmdir(f"tmp/hash_{step}")


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

    return join


def base_sort_join(build_path, probe_path, build_key, probe_key, save_name, keep_key=False, memory_limit=2):
    os.mkdir(f"tmp/sort/{save_name}")
    os.mkdir(f"tmp/sort/{save_name}/subject")
    os.mkdir(f"tmp/sort/{save_name}/object")

    join = []  # heapq and modify it that i distribute it over the disk
    count = 0
    for file_b in sorted(os.listdir(build_path)):
        with open(f"{build_path}/{file_b}", "r") as f:
            Lines = f.readlines()
            for line in Lines:
                text = line.strip()
                build_i = tuple(text.split(" "))

                if build_key == "subject":
                    b_subject, b_object = build_i[0], build_i[1:]
                elif build_key == "object":
                    b_subject, b_object = build_i[:len(build_i) - 1], build_i[-1]

                for file_p in sorted(os.listdir(probe_path)):
                    if sys.getsizeof(join) / 1e6 > memory_limit:  # 2 GiB
                        # store to file
                        print("Memory Limit reached: sorte merge subject")
                        join.sort(key=lambda tup: tup[0])
                        with open(f"tmp/sort/{save_name}/subject/{count}.csv", "w") as f:  # subject
                            for line in join:
                                f.write(" ".join(str(x) for x in line))
                                f.write("\n")
                        if count > 0:
                            chunks = [open(f"tmp/sort/{save_name}/subject/{count - 1}.csv", "r"),
                                      open(f"tmp/sort/{save_name}/subject/{count}.csv", "r")]
                            with open(f"tmp/sort/{save_name}/subject/tmp.csv", 'w') as f_out:
                                f_out.writelines(merge(*chunks, key=lambda k: k.split()[0]))
                            os.remove(f"tmp/sort/{save_name}/subject/{count - 1}.csv")
                            os.remove(f"tmp/sort/{save_name}/subject/{count}.csv")
                            os.rename(f"tmp/sort/{save_name}/subject/tmp.csv", f"tmp/sort/{save_name}/subject/{count}.csv")

                        print("Memory Limit reached: sorte merge object")
                        join.sort(key=lambda tup: tup[-1])
                        with open(f"tmp/sort/{save_name}/object/{count}.csv", "w") as f:  # object
                            for line in join:
                                f.write(" ".join(str(x) for x in line))
                                f.write("\n")

                        if count > 0:
                            chunks = [open(f"tmp/sort/{save_name}/object/{count - 1}.csv", "r"),
                                      open(f"tmp/sort/{save_name}/object/{count}.csv", "r")]
                            with open(f"tmp/sort/{save_name}/object/tmp.csv", 'w') as f_out:
                                f_out.writelines(merge(*chunks, key=lambda k: k.split()[-1]))
                            os.remove(f"tmp/sort/{save_name}/object/{count - 1}.csv")
                            os.remove(f"tmp/sort/{save_name}/object/{count}.csv")
                            os.rename(f"tmp/sort/{save_name}/object/tmp.csv", f"tmp/sort/{save_name}/object/{count}.csv")

                        count += 1
                        del join[:]

                        print("finished sorting\n")
                    start_idx = 0
                    with open(f"{probe_path}/{file_p}", "r") as f:
                        Lines = f.readlines()
                        for idx, line in enumerate(Lines):
                            if idx < start_idx:
                                continue
                            text = line.strip()
                            probe_i = tuple(text.split(" "))

                            if probe_key == "subject":
                                p_subject, p_object = probe_i[0], probe_i[1:]
                            elif probe_key == "object":
                                p_subject, p_object = probe_i[:len(build_i) - 1], probe_i[-1]

                            if build_key == "object" and probe_key == "subject":
                                if b_object == p_subject:
                                    if keep_key:
                                        join.append((*b_subject, b_object, *p_object))
                                    else:
                                        join.append((*b_subject, *p_object))
                                elif p_subject < b_object:
                                    start_idx += 1
                                    continue
                                elif b_object < p_subject:
                                    break

                            elif build_key == "subject" and probe_key == "object":
                                if b_subject == p_object:
                                    if keep_key:
                                        join.append((*p_subject, b_subject, *b_object))
                                    else:
                                        join.append((*p_subject, *b_object))
                                elif p_object < b_subject:
                                    start_idx += 1
                                    continue
                                elif b_subject < p_object:
                                    break

    print("Memory Limit reached: sorte merge subject")
    join.sort(key=lambda tup: tup[0])
    with open(f"tmp/sort/{save_name}/subject/{count}.csv", "w") as f:  # subject
        for line in join:
            f.write(" ".join(str(x) for x in line))
            f.write("\n")
    if count > 0:
        chunks = [open(f"tmp/sort/{save_name}/subject/{count - 1}.csv", "r"),
                  open(f"tmp/sort/{save_name}/subject/{count}.csv", "r")]
        with open(f"tmp/sort/{save_name}/subject/tmp.csv", 'w') as f_out:
            f_out.writelines(merge(*chunks, key=lambda k: k.split()[0]))
        os.remove(f"tmp/sort/{save_name}/subject/{count - 1}.csv")
        os.remove(f"tmp/sort/{save_name}/subject/{count}.csv")
        os.rename(f"tmp/sort/{save_name}/subject/tmp.csv", f"tmp/sort/{save_name}/subject/{count}.csv")

    print("Memory Limit reached: sorte merge object")
    join.sort(key=lambda tup: tup[-1])
    with open(f"tmp/sort/{save_name}/object/{count}.csv", "w") as f:  # object
        for line in join:
            f.write(" ".join(str(x) for x in line))
            f.write("\n")

    if count > 0:
        chunks = [open(f"tmp/sort/{save_name}/object/{count - 1}.csv", "r"),
                  open(f"tmp/sort/{save_name}/object/{count}.csv", "r")]
        with open(f"tmp/sort/{save_name}/object/tmp.csv", 'w') as f_out:
            f_out.writelines(merge(*chunks, key=lambda k: k.split()[-1]))
        os.remove(f"tmp/sort/{save_name}/object/{count - 1}.csv")
        os.remove(f"tmp/sort/{save_name}/object/{count}.csv")
        os.rename(f"tmp/sort/{save_name}/object/tmp.csv", f"tmp/sort/{save_name}/object/{count}.csv")

    count += 1
    del join[:]

    return f"tmp/sort/{save_name}"


def sort_join(**kwargs):
    keep_key = True
    for i in range(1, kwargs["num_joins"] + 1):
        if i == 1:
            join_path = base_sort_join(kwargs[f"build_r_{i}"][kwargs[f"build_key_{i}"]],
                                       kwargs[f"probe_r_{i}"][kwargs[f"probe_key_{i}"]],
                                       kwargs[f"build_key_{i}"], kwargs[f"probe_key_{i}"],
                                       keep_key=keep_key, save_name=i, memory_limit=kwargs["memory_limit"])
        else:
            join_path = base_sort_join(last_join_path + "/" + kwargs[f"build_key_{i}"],
                                       kwargs[f"probe_r_{i}"][kwargs[f"probe_key_{i}"]],
                                       kwargs[f"build_key_{i}"], kwargs[f"probe_key_{i}"],
                                       keep_key=keep_key, save_name=i, memory_limit=kwargs["memory_limit"])
            # use base_hash_join again with abitary num of features but only 1 key
        last_join_path = join_path

    # todo how to implement the conjuctions
    return join_path
