Big Data 2015 - A Study of linguistic drift - Cosine Similarity Distance - Cynthia Oeschger

This MapReduce code computes the distance between a subset of articles from the same year and each year of the corpus based on the cosine similarity.

It takes as arguments :
		- The directory containg the n-grams of the subset of articles
		- The directory containg the n-grams for each year
        - The output directory

A call sample for dating articles from 1995:

hadoop jar CosineArticles.jar ch/epfl/bigdata/CosineArticles /projects/linguistic-shift/articles_samples/15_articles/Corrected/1995 /projects/linguistic-shift/corrected_ngrams/1-grams output


The different directories for the articles are in /projects/linguistic-shift/articles_samples/.
