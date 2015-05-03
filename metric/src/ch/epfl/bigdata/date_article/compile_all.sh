#!/bin/bash

# Script to compile all metrics

# Chi-Square
if [ -d "chi-square" ]; then
    cd "chi-square"
    ./compile.sh
    cd ..
fi

# Cosine
if [ -d "cosine" ]; then
    cd "cosine"
    ./compile.sh
    cd ..
fi

# distance1
if [ -d "distance1" ]; then
    cd "distance1"
    ./compile.sh
    cd ..
fi

# Kullback-Leibler
if [ -d kullback-leibler ]; then
    cd "kullback-leibler"
    sbt package
    cd ..
fi

# select articles
if [ -d select_articles ]; then
    cd "select_articles"
    sbt package
    cd ..
fi