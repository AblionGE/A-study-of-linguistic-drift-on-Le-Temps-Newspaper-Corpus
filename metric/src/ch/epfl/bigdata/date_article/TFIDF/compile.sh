if [ -f TFIDF.jar ]; then
    rm TFIDF.jar
fi

mkdir TFIDF
javac -classpath ${HADOOP_CLASSPATH} -d TFIDF/ src/ch/bigdata2015/linguisticdrift/tfidf/*.java
jar -cvf TFIDF.jar -C TFIDF/ .
rm -rf TFIDF