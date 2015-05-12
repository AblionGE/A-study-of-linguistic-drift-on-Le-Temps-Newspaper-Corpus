Big Data 2015 - A Study of linguistic drift - ‘Out Of Place’ measurement - Tao Lin

This MapReduce code computes the distance between a subset of articles from the same year and each year of the corpus based on the 'Out Of Place' measurement.

It takes as arguments :
        - The directory containg the n-grams of the subset of articles
        - The directory containg the n-grams for each year
        - The output directory

A call sample for dating articles from 1840:

hadoop jar OutofplaceArticle.jar ch/epfl/bigdata/outofplace/Driver /projects/linguistic-shift/articles_samples/15_articles/Corrected/1840 /projects/linguistic-shift/corrected_ngrams/1-grams /projects/linguistic-shift/dateByOutofplace

Reminder: The final result is in the subfolder of output, i.e., /dateByOutofplace/finalResult/

The different directories for the articles are in /projects/linguistic-shift/articles_samples/.