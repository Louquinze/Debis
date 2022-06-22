import re


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


def get_vertical_partitions(keys):
    # edges: follows, friendOf, likes, hasReview
    # nodes: User, Review
    partitions = {key: list() for key in keys}

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
        # Todo Map subject and object to numbers, maybe possible in same loop
        if content[1] in keys:
            partitions[content[1]].append((content[0].replace(" ", ""), content[2].replace(" ", "")))

    return partitions


def base_hash_join(build_r, probe_r, build_key, probe_key, keep_key=False):
    # build_r dont need to have only 2 values ? for easy chain
    # Todo Build phase, map (hash table) join key of relation 1 to the remaining attr.
    hash_table = {}
    for build_i in build_r:
        if build_key == "subject":
            subject, object = build_i[0], build_i[1:]
            if subject not in hash_table:
                hash_table[subject] = set([object])  # create a bucket storing same hash
            else:
                hash_table[subject].add(object)
        elif build_key == "object":
            subject, object = build_i[:len(build_i)-1], build_i[-1]
            if object not in hash_table:
                hash_table[object] = set([subject])
            else:
                hash_table[object].add(subject)

    # Todo Probe phase, look up the new tuples by checking the hash table H(hash_func(key_2))
    join = []
    for subject, object in probe_r:
        if probe_key == "subject":
            if subject in hash_table:
                if keep_key:
                    join += [(*i, subject, object) for i in hash_table[subject]]
                else:
                    join += [(i, object) for i in hash_table[subject]]
        elif probe_key == "object":
            if object in hash_table:
                if keep_key:
                    join += [(*i, object, subject) for i in hash_table[object]]
                else:
                    join += [(i, subject) for i in hash_table[object]]

    return join, hash_table

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
    for i in range(1, kwargs["num_joins"]+1):
        join, hash_table = base_hash_join(kwargs[f"build_r_{i}"], kwargs[f"probe_r_{i}"], kwargs[f"build_key_{i}"], kwargs[f"probe_key_{i}"], keep_key=keep_key)
        buffer.append((join, hash_table))
        if i > 1:
            print(last_join[0], join[0])  # [1:]
            # use base_hash_join again with abitary num of features but only 1 key
        last_join = join

    # todo how to implement the conjuctions
    return buffer
