if [ -f OutofplaceArticle.jar ]; then
    rm OutofplaceArticle.jar
fi

mkdir OutofplaceArticle
javac -classpath ${HADOOP_CLASSPATH} -d OutofplaceArticle/ *.java
jar -cvf OutofplaceArticle.jar -C OutofplaceArticle/ .
rm -rf OutofplaceArticle
