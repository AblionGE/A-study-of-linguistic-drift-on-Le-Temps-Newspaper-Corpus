#!/bin/bash

################ Big Data Course 2015 - A study of linguistic shift - Marc Schaer ####################
### This script runs all metrics over differents subsets of articles in a given year ###
# INPUT : ./date_articles.sh nb_of_articles year nb_of_loop
#       - The number of articles defines the size of the test set
#       - the year is the year where to take the articles (between 1840 and 1998)
#       - The number of loop is how many times the metrics should be run on randomly selected articles
# OUTPUT : This script writes in the file 'mean_error.txt' the mean error for each metric for the given year.
#

EXPECTED_ARGS=3
TEMPORARY_DIRECTORY="hdfs:///projects/linguistic-shift/temp"
KL_ERROR[$3]=0

if [ "$#" -ne $EXPECTED_ARGS ] || [ "$1" -le 0 ]|| [ "$2" -lt 1840 ] || [ "$2" -gt 1995 ] || [ "$3" -le 0 ]; then
  echo "Use: ${0} nb_of_articles year(1840-1995) nb_of_loop(>0)"
  exit
fi

# Removed old result and errors files
if [ -f err_KL ]; then
        rm err_KL
fi

if [ -f err_choose_articles ]; then
        rm err_choose_articles
fi

# Rename previous results file to create a new one
counter=0
while [ -f mean_error.txt_$counter ]
do
        counter=$(($counter+1))
done
mv mean_error.txt mean_error.txt_$counter

for i in `seq "$3"`
do
        echo "Loop" $i
        echo "Selecting articles..."
        if [ -f "select_articles/target/scala-2.10/selectarticle_2.10-1.0.jar" ]; then
                # Create a subset of articles
                spark-submit --class "SelectArticle" --master yarn-cluster --executor-memory 8g --num-executors 50 select_articles/target/scala-2.10/selectarticle_2.10-1.0.jar "$2" "$1" hdfs:///projects/linguistic-shift/corrected_nGramArticle/nGram/ $TEMPORARY_DIRECTORY/articles/$i/ 2>err_choose_articles
        else
                echo "The compilated code of select_articles.scala should be in 'select_articles/target/scala-2.10/selectarticle_2.10-1.0.jar'"
                echo "In select_articles directory, just execute the command 'sbt package'"
                exit
        fi

        echo "Computing Kullback-Leibler Divergence..."
        if [ -f "kullback-leibler/target/scala-2.10/kullback-leibler_2.10-1.0.jar" ]; then
                # Run KL Divergence
                spark-submit --class "KullbackLeiblerArticle" --master yarn-cluster --executor-memory 8g --num-executors 100 kullback-leibler/target/scala-2.10/kullback-leibler_2.10-1.0.jar 1 Corrected $TEMPORARY_DIRECTORY/KL/$i/ $TEMPORARY_DIRECTORY/articles/$i/ "$2" 2>err_KL
        else
                echo "The compilated code of select_articles.scala should be in 'kullback-leibler/target/scala-2.10/kullback-leibler_2.10-1.0.jar'"
                echo "In kullback-leibler directory, just execute the command 'sbt package'"
                hadoop fs -rm -r $TEMPORARY_DIRECTORY/articles/$i/
                exit
        fi

        echo "Getting result and parsing it..."

        # Get Result and create results.csv
        hadoop fs -get $TEMPORARY_DIRECTORY/KL/$i/ && cat $i/* > results.csv

        # Remove local directory
        rm -r $i/

        # Find the smallest distance and add it into an array
        RES=`(cat results.csv | awk 'BEGIN {FS=","}{print $2 " " $3}' | awk 'BEGIN{a=2; b=0}{if ($2<0.0+a) {a=0.0+$2; b=$1}} END{print b}')`
        echo "Real year is $2 and predicted year is $RES"
        KL_ERROR[$i]=$(($RES-$2))
        if [ ${KL_ERROR["$i"]} -lt 0 ]; then
                KL_ERROR[$i]=$(($KL_ERROR[$i] * -1))
        fi

        # Rename the file to keep it for manual verification of minimum and other things in case of failure
        # mv results.csv results.csv_$i
        rm results.csv
done

# Remove temp directory
hadoop fs -rm -r $TEMPORARY_DIRECTORY

# Computing the mean error
echo "Computing the mean error..."
KL_SUM=0
for j in `seq "$3"`
do
        KL_SUM=$(($KL_SUM + ${KL_ERROR[$j]}))
done

KL_MEAN=$(echo "${KL_SUM}/$3" | bc -l)

echo "Mean error for different metrics for $1 of articles in year $2 with $3 iterations" >> mean_error.txt
echo "Mean error in KL : " $KL_MEAN >> mean_error.txt