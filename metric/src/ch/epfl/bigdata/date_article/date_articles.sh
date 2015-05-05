#!/bin/bash

################ Big Data Course 2015 - A study of linguistic shift - Marc Schaer ####################
### This script runs all metrics over differents subsets of articles in a given year ###
# INPUT : ./date_articles.sh nb_of_articles year nb_of_loop
#       - The number of articles defines the size of the test set
#       - the year is the year where to take the articles (between 1840 and 1998)
#       - The number of loop is how many times the metrics should be run on randomly selected articles
# OUTPUT : This script writes in the file 'mean_error.txt' the mean error for each metric for the given year.
#
# IMPORTANT : It's necessary to have the different executables of the metrics in the repository

EXPECTED_ARGS=3
TEMPORARY_DIRECTORY="hdfs:///projects/linguistic-shift/temp"
DISTANCE1_ERROR[$3]=0
KL_ERROR[$3]=0
COSINE_ERROR[$3]=0
CHISQUARE_ERROR[$3]=0
OUTOFPLACE_ERROR[$3]=0

if [ "$#" -ne $EXPECTED_ARGS ] || [ "$1" -le 0 ]|| [ "$2" -lt 1840 ] || [ "$2" -gt 1995 ] || [ "$3" -le 0 ]; then
  echo "Use: ${0} nb_of_articles year(1840-1995) nb_of_loop(>0)"
  exit
fi

# Removed old result and errors files
if [ -f err_Distance1 ]; then
        rm err_Distance1
fi

if [ -f err_KL ]; then
        rm err_KL
fi

if [ -f err_Cosine ]; then
        rm err_Cosine
fi

if [ -f err_ChiSquare ]; then
        rm err_ChiSquare
fi

if [ -f err_OUTOFPLACE ]; then
        rm err_OUTOFPLACE
fi

if [ -f err_choose_articles ]; then
        rm err_choose_articles
fi

# Rename previous results file to create a new one
if [ -f mean_error.txt ]; then
    counter=0
    while [ -f mean_error.txt_$counter ]
    do
            counter=$(($counter+1))
    done
    mv mean_error.txt mean_error.txt_$counter
fi

for i in `seq "$3"`
do
        echo "Iteration nr." $i

######################## SELECTING ARTICLES ##############################
        echo "Selecting articles..."
        if [ -f "select_articles/target/scala-2.10/selectarticle_2.10-1.0.jar" ]; then
                # Create a subset of articles
                spark-submit --class "SelectArticle" --master yarn-cluster --executor-memory 8g --num-executors 50 select_articles/target/scala-2.10/selectarticle_2.10-1.0.jar "$2" "$1" hdfs:///projects/linguistic-shift/corrected_nGramArticle/nGram/ $TEMPORARY_DIRECTORY/articles/${i}/${2} 2>err_choose_articles
        else
                echo "The compilated code of select_articles.scala should be in 'select_articles/target/scala-2.10/selectarticle_2.10-1.0.jar'"
                echo "In select_articles directory, just execute the command 'sbt package'"
                exit
        fi

######################## DISTANCE1 ##############################
        echo "Computing Distance1..."
        if [ -d "distance1" ]; then
            if [ -f "distance1/Distance1Articles.jar" ]; then
                # Run Distance1
                hadoop jar distance1/Distance1Articles.jar ch/epfl/bigdata/Distance1Articles $TEMPORARY_DIRECTORY/articles/$i/$2 /projects/linguistic-shift/corrected_ngrams/1-grams $TEMPORARY_DIRECTORY/Distance1/$i 2>err_Distance1
            else
                cd "distance1"
                ./compile.sh
                cd ..
                hadoop jar distance1/Distance1Articles.jar ch/epfl/bigdata/Distance1Articles $TEMPORARY_DIRECTORY/articles/$i/$2 /projects/linguistic-shift/corrected_ngrams/1-grams $TEMPORARY_DIRECTORY/Distance1/$i 2>err_Distance1
            fi
        else
            echo "No jar for Distance1Articles"
            hadoop fs -rm -r $TEMPORARY_DIRECTORY
            exit
        fi
        echo "Getting result of Distance1 and parsing it..."
        # Get Result and create results.csv for Distance1
        hadoop fs -get $TEMPORARY_DIRECTORY/Distance1/$i/ && cat $i/* > results_D1.csv
        rm -r $i/

        # Find the smallest distance and add it into an array for Distance1
        RES=`(cat results_D1.csv | awk 'BEGIN {FS=","}{print $2 " " $3}' | awk 'BEGIN{a=2; b=0}{if ($2<0.0+a) {a=0.0+$2; b=$1}} END{print b}')`
        echo "Real year is $2 and predicted year is $RES"
        DISTANCE1_ERROR[$i]=$(($RES-$2))
        if [ ${DISTANCE1_ERROR["$i"]} -lt 0 ]; then
                DISTANCE1_ERROR[$i]=$(echo "${DISTANCE1_ERROR[$i]} * -1" | bc -l)
        fi

######################## COSINE ##############################
        echo "Computing Cosine Metric..."
        if [ -d "cosine" ]; then
            if [ -f "cosine/CosineArticles.jar" ]; then
                # Run Cosine
                hadoop jar cosine/CosineArticles.jar ch/epfl/bigdata/CosineArticles $TEMPORARY_DIRECTORY/articles/$i/$2 /projects/linguistic-shift/corrected_ngrams/1-grams $TEMPORARY_DIRECTORY/Cosine/$i 2>err_Cosine
            else
                cd "cosine"
                ./compile.sh
                cd ..
                hadoop jar cosine/CosineArticles.jar ch/epfl/bigdata/CosineArticles $TEMPORARY_DIRECTORY/articles/$i/$2 /projects/linguistic-shift/corrected_ngrams/1-grams $TEMPORARY_DIRECTORY/Cosine/$i 2>err_Cosine
            fi
        else
            echo "No jar for CosineArticles"
            hadoop fs -rm -r $TEMPORARY_DIRECTORY
            exit
        fi
        echo "Getting result of CosineArticles and parsing it..."
        # Get Result and create results.csv for Cosine
        hadoop fs -get $TEMPORARY_DIRECTORY/Cosine/$i/ && cat $i/* > results_Cos.csv
        rm -r $i/

        # Find the smallest distance and add it into an array for Cosine
        RES=`(cat results_Cos.csv | awk 'BEGIN {FS=","}{print $2 " " $3}' | awk 'BEGIN{a=2; b=0}{if ($2<0.0+a) {a=0.0+$2; b=$1}} END{print b}')`
        echo "Real year is $2 and predicted year is $RES"
        COSINE_ERROR[$i]=$(($RES-$2))
        if [ ${COSINE_ERROR["$i"]} -lt 0 ]; then
                COSINE_ERROR[$i]=$(echo "${COSINE_ERROR[$i]} * -1" | bc -l)
        fi


######################## CHISQUARE ##############################
        echo "Computing ChiSquare..."
        if [ -d "chi-square" ]; then
            if [ -f "chi-square/ChiSquareArticles.jar" ]; then
                # Run ChiSquare
                hadoop jar chi-square/ChiSquareArticles.jar ch/epfl/bigdata/ChiSquareArticles $TEMPORARY_DIRECTORY/articles/$i/$2 /projects/linguistic-shift/corrected_ngrams/1-grams $TEMPORARY_DIRECTORY/ChiSquare/$i 2>err_ChiSquare
            else
                cd "chi-square"
                ./compile.sh
                cd ..
                hadoop jar chi-square/ChiSquareArticles.jar ch/epfl/bigdata/ChiSquareArticles $TEMPORARY_DIRECTORY/articles/$i/$2 /projects/linguistic-shift/corrected_ngrams/1-grams $TEMPORARY_DIRECTORY/ChiSquare/$i 2>err_ChiSquare
            fi
        else
            echo "No jar for ChiSquare"
            hadoop fs -rm -r $TEMPORARY_DIRECTORY
            exit
        fi
        echo "Getting result of ChiSquare and parsing it..."
        # Get Result and create results.csv for ChiSquare
        hadoop fs -get $TEMPORARY_DIRECTORY/ChiSquare/$i/ && cat $i/* > results_ChiSquare.csv
        rm -r $i/

        # Find the smallest distance and add it into an array for ChiSquare
        RES=`(cat results_ChiSquare.csv | awk 'BEGIN {FS=","}{print $2 " " $3}' | awk 'BEGIN{a=2; b=0}{if ($2<0.0+a) {a=0.0+$2; b=$1}} END{print b}')`
        echo "Real year is $2 and predicted year is $RES"
        CHISQUARE_ERROR[$i]=$(($RES-$2))
        if [ ${CHISQUARE_ERROR["$i"]} -lt 0 ]; then
                CHISQUARE_ERROR[$i]=$(echo "${CHISQUARE_ERROR[$i]} * -1" | bc -l)
        fi

######################## OUTOFPLACE ##############################
        echo "Computing OutOfPlace..."
        if [ -d "outofplace" ]; then
            if [ -f "outofplace/OutofplaceArticle.jar" ]; then
                # Run Distance OutOfPlace
                hadoop jar outofplace/OutofplaceArticle.jar ch.epfl.bigdata.outofplace.Driver $TEMPORARY_DIRECTORY/articles/$i/$2 /projects/linguistic-shift/corrected_ngrams/1-grams $TEMPORARY_DIRECTORY/OUTOFPLACE/$i 2>err_OUTOFPLACE       
            else
                cd "outofplace"
                ./compile.sh
                cd ..
                hadoop jar outofplace/OutofplaceArticle.jar ch.epfl.bigdata.outofplace.Driver $TEMPORARY_DIRECTORY/articles/$i/$2 /projects/linguistic-shift/corrected_ngrams/1-grams $TEMPORARY_DIRECTORY/OUTOFPLACE/$i 2>err_OUTOFPLACE       
            fi
        else
            echo "No jar for OutOfPlace"
            hadoop fs -rm -r $TEMPORARY_DIRECTORY
            exit
        fi
        echo "Getting result of OutOfPlace and parsing it..."
        # Get Result and create results.csv for OUTOFPLACE
        hadoop fs -get $TEMPORARY_DIRECTORY/OUTOFPLACE/$i/finalResult && cat finalResult/* > results_OUTOFPLACE.csv
        rm -r "finalResult"

        # Find the smallest distance and add it into an array for OutOfPlace
        RES=`(cat results_OUTOFPLACE.csv | awk 'BEGIN {FS=","}{print $2 " " $3}' | awk 'BEGIN{a=2; b=0}{if ($2<0.0+a) {a=0.0+$2; b=$1}} END{print b}')`
        echo "Real year is $2 and predicted year is $RES"
        OUTOFPLACE_ERROR[$i]=$(($RES-$2))
        if [ ${OUTOFPLACE_ERROR["$i"]} -lt 0 ]; then
                OUTOFPLACE_ERROR[$i]=$(echo "${OUTOFPLACE_ERROR[$i]} * -1" | bc -l)
        fi

######################## KULLBACK-LEIBLER ##############################
        echo "Computing Kullback-Leibler Divergence..."
        if [ -f "kullback-leibler/target/scala-2.10/kullback-leibler_2.10-1.0.jar" ]; then
                # Run KL Divergence
                spark-submit --class "KullbackLeiblerArticle" --master yarn-cluster --executor-memory 8g --num-executors 100 kullback-leibler/target/scala-2.10/kullback-leibler_2.10-1.0.jar 1 Corrected $TEMPORARY_DIRECTORY/KL/$i/ $TEMPORARY_DIRECTORY/articles/$i/ "$2" 2>err_KL
        else
                echo "The compilated code of select_articles.scala should be in 'kullback-leibler/target/scala-2.10/kullback-leibler_2.10-1.0.jar'"
                echo "In kullback-leibler directory, just execute the command 'sbt package'"
                hadoop fs -rm -r $TEMPORARY_DIRECTORY
                exit
        fi
        echo "Getting result of KL and parsing it..."
        # Get Result and create results.csv for KL
        hadoop fs -get $TEMPORARY_DIRECTORY/KL/$i/ && cat $i/* > results_KL.csv
        rm -r $i/

        # Find the smallest distance and add it into an array for KL
        RES=`(cat results_KL.csv | awk 'BEGIN {FS=","}{print $2 " " $3}' | awk 'BEGIN{a=2; b=0}{if ($2<0.0+a) {a=0.0+$2; b=$1}} END{print b}')`
        echo "Real year is $2 and predicted year is $RES"
        KL_ERROR[$i]=$(($RES-$2))
        if [ ${KL_ERROR["$i"]} -lt 0 ]; then
                KL_ERROR[$i]=$(echo "${KL_ERROR[$i]} * -1" | bc -l)
        fi

        # Rename the file to keep it for manual verification of minimum and other things in case of failure
        # mv results.csv results.csv_$i
done
rm results*.csv

# Remove temp directory
hadoop fs -rm -r $TEMPORARY_DIRECTORY

# Computing the mean error
echo "Computing the mean error..."
D1_SUM=0
KL_SUM=0
COS_SUM=0
CHI_SUM=0
OUT_SUM=0
for j in `seq "$3"`
do
        KL_SUM=$(($KL_SUM + ${KL_ERROR[$j]}))
        D1_SUM=$(($D1_SUM + ${DISTANCE1_ERROR[$j]}))
        COS_SUM=$(($COS_SUM + ${COSINE_ERROR[$j]}))
        CHI_SUM=$(($CHI_SUM + ${CHISQUARE_ERROR[$j]}))
        OUT_SUM=$(($OUT_SUM + ${OUTOFPLACE_ERROR[$j]}))
done

KL_MEAN=$(echo "${KL_SUM}/$3" | bc -l)
D1_MEAN=$(echo "${D1_SUM}/$3" | bc -l)
COS_MEAN=$(echo "${COS_SUM}/$3" | bc -l)
CHI_MEAN=$(echo "${CHI_SUM}/$3" | bc -l)
OUT_MEAN=$(echo "${OUT_SUM}/$3" | bc -l)

echo "Writing in mean_error.txt..."
echo "Mean error for different metrics for $1 articles in year $2 with $3 iterations" >> mean_error.txt
echo "Distance1 :" $D1_MEAN >> mean_error.txt
echo "Cosine :" $COS_MEAN >> mean_error.txt
echo "Chi-Square :" $CHI_MEAN >> mean_error.txt
echo "Kullback-Leibler :" $KL_MEAN >> mean_error.txt
echo "OutOfPlace :" $OUT_MEAN >> mean_error.txt

echo "Done!"