Big Data 2015 - A Study of linguistic drift - Cosine Similarity Distance - Cynthia Oeschger

This MapReduce code computes the distance between a subset of articles from the same year and each year of the corpus based on the cosine similarity using TF-IDF.

It takes as arguments :
		- The directory containg the n-grams of the subset of articles with their TF-IDF value.
		- The directory containg the n-grams for each year with their TF-IDF value.
        - The output directory

The different directories for the articles are in /projects/linguistic-shift/articles_samples/ and should be executed with the TFIDF code in date_article first, to obtain the TFIDF value of the words.
