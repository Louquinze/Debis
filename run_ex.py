import os
os.system("mkdir tmp")

os.system("mkdir res_1")
os.system("python main.py --type hash --dataset small")
os.system("mv tmp/3/subject/*csv res_1")
os.system("rm -r tmp")
os.system("mkdir tmp")

os.system("mkdir res_2")
os.system("python main.py --type sort --dataset small")
os.system("mv tmp/3/subject/*csv res_2")
os.system("rm -r tmp")
os.system("mkdir tmp")

os.system("mkdir res_3")
os.system("python main.py --type hash --dataset huge")
os.system("mv tmp/3/subject/*csv res_3")
os.system("rm -r tmp")
os.system("mkdir tmp")

os.system("mkdir res_4")
os.system("python main.py --type sort --dataset huge")
os.system("mv tmp/3/subject/*csv res_4")
os.system("rm -r tmp")
