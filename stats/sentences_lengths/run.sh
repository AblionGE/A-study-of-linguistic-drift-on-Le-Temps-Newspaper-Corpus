#!/usr/bin/env zsh
#usage: 
# ./run.sh input_dir output_dir
hadoop jar Ngram.jar ch.epfl.bigdata.ngram.Ngram "$1" "$2" 15
