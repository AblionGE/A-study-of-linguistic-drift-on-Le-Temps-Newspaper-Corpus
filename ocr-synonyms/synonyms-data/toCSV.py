import re

inp = open("synonyms.txt")
out = open("synonyms.csv", 'w')

def wrap(s):
	return '"'+s+'"'
def replaceAccents(s):
        return re.sub('à', 'a',re.sub('à', 'a', re.sub(r'[éè]', 'e', s)))

lines = inp.readlines()
index = 1
for (i,line) in zip(range(1,len(lines)+1), lines):
	cleaned = list(filter(lambda s:s!="" and s!="\n", line.split("\t")))
	out.write(",".join(map(lambda s:wrap(re.sub('\n','', s)), cleaned)))
	out.write("\n")

inp.close()
out.close()
print("done")


