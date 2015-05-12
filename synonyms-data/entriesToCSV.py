import re

inp = open("entities.txt")
out = open("entities.csv", 'w')

def wrap(s):
	return '"'+s+'"'
def replaceAccents(s):
	v1 = re.sub(r'[àâ]', 'a',re.sub('ô', 'o', re.sub(r'[éèêë]', 'e', s)))
	v2 = re.sub(r'[ûù]', 'u', re.sub(r'[îï]', 'i',v1))
	return re.sub('\.', '',re.sub(r'[-\' ]', ',', v2))

lines = inp.readlines()
for (i,line) in zip(range(1,len(lines)+1), lines):
	parts = re.sub('½', 'oe', re.sub('\n','',line)).lower().split("\t")
	word = parts[0]
	cleaned = replaceAccents(word)
	out.write(",".join([wrap(str(i)), wrap(cleaned), wrap(word), wrap(replaceAccents(re.sub('\|',';', parts[1])))]))
	out.write("\n")

inp.close()
out.close()
print("done")


