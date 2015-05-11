#!/bin/bash
# Script to compile distance1

if [ -f Distance1Articles.jar ]; then
    rm Distance1Articles.jar
fi

mkdir Distance1Articles
javac -classpath ${HADOOP_CLASSPATH} -d Distance1Articles/ Distance1Articles.java
jar -cvf Distance1Articles.jar -C Distance1Articles/ .
rm -rf Distance1Articles