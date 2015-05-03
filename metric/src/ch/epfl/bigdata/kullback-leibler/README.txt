Big Data 2015 - A Study of linguistic drift - Kullback-Leibler Divergence - Marc Schaer

This code computes the Kullback-Leibler Divergence between all years.

It takes as arguments :
        - The number of grams considered (1,2 or 3, but only 1 for the moment)
        - If the corpus used is the corpus with the "Corrected" OCR or "WithoutCorrection"
        - If you want to compute with the "Probability" of each word or with its "TFIDF" value
        - The output directory

A call sample (You have to be in the directory where the code is and you have to compile it with 'sbt package') :

spark-submit --class "KullbackLeibler" --master yarn-cluster --executor-memory 8g --num-executors 100 target/scala-2.10/kullback-leibler_2.10-1.0.jar 1 Corrected Probability hdfs:///user/your_username/ 2>err

You have to assume that the articles directory exists and contains files.

IMPORTANT : This code needs to have the following directory with data :
        - /projects/linguistic-shift/stats/Corrected/ProbabilityOfAWordOverAllYears/
        - /projects/linguistic-shift/stats/WithoutCorrection/ProbabilityOfAWordOverAllYears/
        - /projects/linguistic-shift/stats/Corrected/ProbabilityOfAWordPerYear/
        - /projects/linguistic-shift/stats/WithoutCorrection/ProbabilityOfAWordPerYear/