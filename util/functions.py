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


def base_hash_join(build_r, probe_r, build_key, probe_key):
    # Todo Build phase, map (hash table) join key of relation 1 to the remaining attr.
    hash_table = {}
    for subject, object in build_r:
        if build_key == "subject":
            hash_table[subject] = object
        elif build_key == "object":
            hash_table[object] = subject

    # Todo Probe phase, look up the new tuples by checking the hash table H(hash_func(key_2))
    join = []
    for subject, object in probe_r:
        if probe_key == "subject":
            if subject in hash_table:
                join.append((hash_table[subject], object))
        elif probe_key == "object":
            if object in hash_table:
                join.append((hash_table[object], subject))

    return join