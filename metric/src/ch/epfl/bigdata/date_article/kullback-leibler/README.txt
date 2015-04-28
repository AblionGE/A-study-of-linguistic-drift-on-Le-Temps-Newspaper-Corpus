Big Data 2015 - A Study of linguistic drift - Date a set of article with Kullback-Leibler Divergence - Marc Schaer

This code computes the Kullback-Leibler Divergence between a subset of articles and each year of the corpus.
It takes as arguments :
        - The number of grams considered (1,2 or 3, but only 1 for the moment)
        - If the corpus used is the corpus with the "Corrected" OCR or "WithoutCorrection"
        - The input format of the file ("Java" or "Spark")
                - You should set this argument to "Java" if you use the result from TFIDF
        - The output directory
        - The directory where the articles are.
                - The articles have to be of the form : (word,occurrences)

A call sample (You have to be in the directory where the code is and you have to compile it with 'sbt package') :

spark-submit --class "KullbackLeiblerArticle" --master yarn-cluster --executor-memory 8g --num-executors 50 target/scala-2.10/kullback-leibler_2.10-1.0.jar 1 Corrected Spark hdfs:///projects/linguistic-shift/Kullback-Leibler/article/Corrected/1-grams/1840/ hdfs:///projects/linguistic-shift/articles_samples/15/Corrected/1840/ 2>err

You have to assume that the articles directory exists and contains files.

The different directories for the articles are in /projects/linguistic-shift/articles_samples/.
