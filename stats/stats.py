#!/usr/bin/env python
# -*- coding: utf-8 -*-
import subprocess
import sys
import multiprocessing

ROOT_DIR='/projects/linguistic-shift/cor_ngrams/1-grams'
START_YEAR=1840
END_YEAR=1998

def get_occurence(year):
    year = str(year)
    ngram = sys.argv[1]
    occurences = subprocess.check_output("hadoop fs -cat '%s/*%s*' | grep -P '[\t]%s$' | awk '{print $1}'" % (ROOT_DIR, year, ngram), shell=True).strip()
    return int(occurences.strip() or 0)

pool = multiprocessing.Pool(7)
years_occurences = zip(range(START_YEAR, END_YEAR), pool.map_async(get_occurence, range(START_YEAR, END_YEAR)).get(999999999))
print(years_occurences)
max_occurence = max(map(lambda a: a[1], years_occurences))
for year, occurence in years_occurences:
    print(str(year) + ": " + '#' * int((float(occurence) / float(max_occurence)) * 40))
