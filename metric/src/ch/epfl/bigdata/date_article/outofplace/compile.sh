if [ -f OutofplaceArticle.jar ]; then
    rm OutofplaceArticle.jar
fi

mkdir OutofplaceArticle
javac -classpath ${HADOOP_CLASSPATH} -d OutofplaceArticle/ CalMapper.java CalReducer.java CombinationKey.java CombineResultMapper.java CombineResultReducer.java DefinedComparator.java DefinedGroupSort.java DefinedPartition.java Driver.java FinalResultMapper.java FinalResultReducer.java GetDataMapper.java GetDataReducer.java PrepareDataMapper.java PrepareDataReducer.java
jar -cvf OutofplaceArticle.jar -C OutofplaceArticle/ .
rm -rf OutofplaceArticle
