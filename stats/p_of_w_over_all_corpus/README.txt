Big Data 2015 - A Study of linguistic drift - Probability to have a Word w in all the corpus - Marc Schaer

This code computes the probability of a Word over all the corpus.
It simply sums all the words that are the same from each year and then it divides, for each word, the sum obtained by the total of words in the corpus.

It takes as arguments :
        - A directory with all the corpus
        - An output directory

The input directory can be :
        - /projects/linguistic-shift/corrected_ngrams/
        - /projects/linguistic-shift/ngrams/

A call sample (You have to be in the directory where the code is and you have to compile it with 'sbt package') :

spark-submit --class "ProbabilityOfAWordInAllCorpus" --master yarn-cluster --executor-memory 8g --num-executors 50 * target/scala-2.10/probabilityofawordinallcorpus_2.10-1.0.jar hdfs:///projects/linguistic-shift/corrected_ngrams/1-grams/ hdfs:///user/your_username/ 2>err