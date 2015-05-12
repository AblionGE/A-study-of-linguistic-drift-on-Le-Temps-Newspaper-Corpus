if [ -f ocrcorrection.jar ]; then
    rm ocrcorrection.jar
fi

mkdir ocrcorrection
javac -classpath ${HADOOP_CLASSPATH} -d ocrcorrection/ *.java
jar -cvf ocrcorrection.jar -C ocrcorrection/ .
rm -rf ocrcorrection
