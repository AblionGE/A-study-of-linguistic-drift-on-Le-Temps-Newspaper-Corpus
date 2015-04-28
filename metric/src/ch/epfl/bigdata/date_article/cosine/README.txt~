Big Data 2015 - A Study of linguistic drift - Chi-Square Distance - Cynthia Oeschger

This MapReduce code computes the chi-square distance between a subset of articles from the same year and each year of the corpus.

It takes as arguments :
		- The directory containg the n-grams of the subset of articles
		- The directory containg the n-grams for each year
        - The output directory

A call sample for dating articles from 1995:

hadoop jar ChiSquareArticles.jar ch/epfl/bigdata/ChiSquareArticles /projects/linguistic-shift/articles_samples/15_articles/Corrected/1995 /projects/linguistic-shift/corrected_ngrams/1-grams output


IMPORTANT : These files must exist in order to run the application :
        - /projects/linguistic-shift/stats/1-grams-TotOccurenceYear/YearOccurences
        - /projects/linguistic-shift/stats/WordOccurenceOverAllYears

The different directories for the articles are in /projects/linguistic-shift/articles_samples/.
