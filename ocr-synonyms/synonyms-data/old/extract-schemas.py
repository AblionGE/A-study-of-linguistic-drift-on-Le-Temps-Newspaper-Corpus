import re

inp = open("syns.txt")
cat = open("categories.csv", 'w')
entries = open("entries.csv", 'w')
words = open("words.csv", 'w')

def wrap(s):
	return '"'+s+'"'
def replaceAccents(s):
        return re.sub('à', 'a',re.sub('à', 'a', re.sub(r'[éè]', 'e', s)))

lines = inp.readlines()
index = 1
for (i,line) in zip(range(1,len(lines)+1), lines):
	cleaned = list(filter(lambda s:s!="" and s!="\n", line.split("\t")))
	entry_id = wrap(str(i))
	entries.write(",".join([wrap(str(i)), wrap(cleaned[0])]))
	entries.write("\n")
	for category in cleaned[1:]:
		parts = category.split(":")
		name = parts[0].split("[")[0].strip()
		words.write(re.sub(' ', ',', name))
		words.write("\n")
		synonyms = parts[1] if len(parts)>1 else ""
		synonyms_list = filter(lambda s:s!="", map(lambda s:s.strip(), synonyms.split(";")))
		for synonym in synonyms_list:
			words.write(re.sub(' ', ',', synonym))
			words.write("\n")
		cat.write(",".join([wrap(str(index)), entry_id, wrap(name), wrap(synonyms)]))
		cat.write("\n")
		index += 1

inp.close()
cat.close()
entries.close()
words.close()


