Big Data 2015 - A Study of linguistic drift - Cosine Similarity Distance - Cynthia Oeschger

This MapReduce code computes the a distance between two years based on the cosine similarity.

It takes as arguments :
        - The input directory containg the n-grams for each year
        - The output directory

A call sample :

hadoop jar Cosine.jar ch/epfl/bigdata/Cosine /projects/linguistic-shift/corrected_ngrams/1-grams output

