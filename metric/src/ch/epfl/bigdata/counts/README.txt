Big Data 2015 - A Study of linguistic drift - Counts - Cynthia Oeschger

WordFrequnecyOverAllYears.java computes the count of each word within all the years of the corpus.
YearCardinality.java computes the cardinality (the number of distinct words) for each year of the corpus.

It takes as arguments :
        - The input directory containg the n-grams for each year
        - The output directory

A call sample :

hadoop jar YearCardinality.jar ch/epfl/bigdata/YearCardinality /projects/linguistic-shift/corrected_ngrams/1-grams output

