#!/usr/bin/env python
# -*- coding: utf-8 -*-
import subprocess
import sys
import multiprocessing

ROOT_DIR='/projects/linguistic-shift/cor_ngrams/2-grams'
START_YEAR=1840
END_YEAR=1998

def get_occurence(year):
    year = str(year)
    #root_dir = ROOT_DIR % ngram_size
    occurences = subprocess.check_output("hadoop fs -cat '%s/*%s*' | grep -c -P '1\t'" % (ROOT_DIR, year), shell=True).strip()

    total_occurences = subprocess.check_output("hadoop fs -cat '%s/*%s*' | wc -l" % (ROOT_DIR, year), shell=True).strip()

    return int(occurences.strip() or 0) / float(int(total_occurences.strip()))

pool = multiprocessing.Pool(7)
years_occurences = zip(range(START_YEAR, END_YEAR), pool.map_async(get_occurence, range(START_YEAR, END_YEAR)).get(999999999))
for year, occurence in years_occurences:
    print(str(year) + ": " + '#' * int(occurence * 40))
