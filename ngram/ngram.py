#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"Read SequenceFile encoded as TypedBytes, store files by key (assumes they are unique) with data as the value"
import hadoopy
from lxml import etree
import os
from collections import defaultdict
import sys
 
local_path = 'output_dir'
 
def main(ngram_size=1):
    whole_file = "".join(map(lambda a: a[1], hadoopy.readtb(sys.argv[1])))
    root_tree = etree.fromstring(whole_file)
    ngram = defaultdict(lambda : 0)
    for article in root_tree.xpath('//article'):
        current_ngram = []
        for word in article.text.split(' '):
            current_ngram.append(word)
            if len(current_ngram) == ngram_size:
                ngram[" ".join(current_ngram)] += 1
                current_ngram.pop()
    print(dict(ngram))


if __name__ == '__main__':
    main()
