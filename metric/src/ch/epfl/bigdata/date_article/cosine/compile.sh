#!/bin/bash
# Script to compile CosineArticles

if [ -f CosineArticles.jar ]; then
    rm CosineArticles.jar
fi

mkdir CosineArticles
javac -classpath ${HADOOP_CLASSPATH} -d CosineArticles/ CosineArticles.java
jar -cvf CosineArticles.jar -C CosineArticles/ .
rm -rf CosineArticles