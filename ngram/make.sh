#!/usr/bin/env zsh
rm *.jar
javac -classpath $(hadoop classpath) src/**/*.java -d classes
pushd classes
jar cf ../Ngram.jar **/*.class
popd
rm -R classes/**/*.class
