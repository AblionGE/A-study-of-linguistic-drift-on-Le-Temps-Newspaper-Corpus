Big Data 2015 - A Study of linguistic drift - Date a set of article with euclidean distance on punctuation and sentences lengths statistics - Gil Brechbühler

This code computes the euclidean distance for each year for a set of articles. It uses some statistics like the mean sentence length, mean number of comas/colons/semicolons.
It takes as arguments :
        - The year to take a sample of articles to date
        - The size of the sample (number of articles to date)
        - The output directory

A call sample (You have to be in the directory where the code is and you have to compile it with 'sbt package') :

spark-submit --class "PunctSentencesMetric" --master yarn-cluster --num-executors 50 target/scala-2.10/punctuation-sentences-metric_2.10-1.0.jar 1888 20 hdfs:///projects/linguistic-shift/gilMetricTest

IMPORTANT : This code needs to have the following files and directory exist and are correct :
        - hdfs:///projects/linguistic-shift/stats/sentencesLength/means.csv
        - hdfs:///projects/linguistic-shift/stats/punctuationStats/stats.csv
		- hdfs:///projects/linguistic-shift/articles_separated/
