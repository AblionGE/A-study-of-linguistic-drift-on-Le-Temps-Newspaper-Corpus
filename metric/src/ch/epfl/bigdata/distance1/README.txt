Big Data 2015 - A Study of linguistic drift - First Simple Metric - Cynthia Oeschger and Farah Bouassida

This MapReduce code computes a simple distance between two years based on the common words used.

It takes as arguments :
        - The input directory containg the n-grams for each year
        - The output directory

A call sample :

hadoop jar DistanceComputation.jar ch/epfl/bigdata/DistanceComputation /projects/linguistic-shift/corrected_ngrams/1-grams output

You have to assume that the articles directory exists and contains files.

IMPORTANT : This file must exist in order to run the application :
        - /projects/linguistic-shift/stats/YearCardinality
