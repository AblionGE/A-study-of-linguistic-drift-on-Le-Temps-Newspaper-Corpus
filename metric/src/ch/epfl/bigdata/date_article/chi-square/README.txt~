Big Data 2015 - A Study of linguistic drift - Date a set of article with a simple distance - Cynthia Oeschger

This MapReduce code computes a simple distance between a subset of articles from the same year and each year of the corpus based on the common words used.

It takes as arguments :
		- The directory containg the n-grams of the subset of articles
		- The directory containg the n-grams for each year
        - The output directory

A call sample for dating articles from 1995:

hadoop jar Distance1Articles.jar ch/epfl/bigdata/Distance1Articles /projects/linguistic-shift/articles_samples/15_articles/Corrected/1995 /projects/linguistic-shift/corrected_ngrams/1-grams output


IMPORTANT : This file must exist in order to run the application :
        - /projects/linguistic-shift/stats/YearCardinality

The different directories for the articles are in /projects/linguistic-shift/articles_samples/.
