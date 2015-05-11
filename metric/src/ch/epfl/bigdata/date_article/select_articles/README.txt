Big Data 2015 - A Study of linguistic drift - Select a subset of articles in a year - Marc Schaer

This code takes a subset of article in a given year, it combines the words (sum their occurrences) and return the list of words of the selected articles

It takes as arguments :
        - The year
        - The number of articles you want
        - The directory where the articles are
                - The articles have to be of the form : (word,occurrences)
        - The output directory

A call sample (You have to be in the directory where the code is and you have to compile it with 'sbt package') :

spark-submit --class "SelectArticle" --master yarn-cluster --executor-memory 8g --num-executors 50 target/scala-2.10/selectarticle_2.10-1.0.jar 1995 15 hdfs:///projects/linguistic-shift/corrected_nGramArticle/ hdfs:///user/your_username/

The different directories for the input directory are :
        - /projects/linguistic-shift/corrected_nGramArticle/
        - /projects/linguistic-shift/nGramArticle/nGram/

The output is of form : word \tab occurrences