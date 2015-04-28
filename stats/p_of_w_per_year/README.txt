Big Data 2015 - A Study of linguistic drift - Probability to have a Word w per year - Marc Schaer

This code computes for each year the probability that a word appears.

It takes as arguments :
        - A directory for all the corpus
        - An output directory

The input directories can be :
        - /projects/linguistic-shift/ngrams/
        - /projects/linguistic-shift/corrected_ngrams/

A call sample (You have to be in the directory where the code is and you have to compile it with 'sbt package') :

spark-submit --class "ProbabilityOfAWordPerYear" --master yarn-cluster --executor-memory 8g --num-executors 50 target/scala-2.10/probabilityofawordperyear_2.10-1.0.jar 1 hdfs:///projects/linguistic-shift/ngrams/1-grams/ hdfs:///projects/linguistic-shift/stats/WithoutCorrection/ProbabilityOfAWordPerYear/1-grams/