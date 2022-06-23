import pickle
import heapq

MAXPERFILE = 100  # the array will never get bigger than this

def getfname(i):
    return "pickled%d.dat" % i

filenum = 0
def dumptofile(a):  # dump the array to file, as pickled data
    global filenum
    fname = getfname(filenum)
    with open(fname, "wb") as f:
        pickle.dump(len(a), f)
        for x in a:
            pickle.dump(x, f)
    filenum += 1

# generate some random data
import random
a = []
for _ in range(1012):  # 10 "full" files with some leftovers
    a.append(random.random())
    if len(a) == MAXPERFILE:
        a.sort(reverse=True)
        dumptofile(a)
        del a[:]
if a:
    a.sort(reverse=True)
    dumptofile(a)

print("number of files written:", filenum)

# now merge the files together; first a function
# to generate the file contents, one at a time
def feedfile(i):
    fname = getfname(i)
    with open(fname, "rb") as f:
        count = pickle.load(f)
        for _ in range(count):
            yield pickle.load(f)

for x in heapq.merge(*(feedfile(i) for i in range(filenum)),
                     reverse=True):
    print(x)