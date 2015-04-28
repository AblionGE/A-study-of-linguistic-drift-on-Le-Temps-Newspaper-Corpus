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
    top_n = sys.argv[1]
    occurences = subprocess.check_output("hadoop fs -cat '%s/*%s*' | tail -%s" % (ROOT_DIR, year, top_n), shell=True).strip()
    return map(lambda a: ''.join(map(lambda b: b, a.split('\t')[1])), occurences.split('\n'))

pool = multiprocessing.Pool(7)
years_top_grams = zip(range(START_YEAR, END_YEAR), pool.map_async(get_occurence, range(START_YEAR, END_YEAR)).get(999999999))

for year, top_ngram in years_top_grams:
    print(str(year) + ": " + ",".join(top_ngram))

