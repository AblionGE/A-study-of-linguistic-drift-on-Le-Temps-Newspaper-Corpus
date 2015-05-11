#!/bin/bash
# Script to compile ChiSquareArticles

if [ -f ChiSquareArticles.jar ]; then
    rm ChiSquareArticles.jar
fi

mkdir ChiSquareArticles
javac -classpath ${HADOOP_CLASSPATH} -d ChiSquareArticles/ ChiSquareArticles.java
jar -cvf ChiSquareArticles.jar -C ChiSquareArticles/ .
rm -rf ChiSquareArticles