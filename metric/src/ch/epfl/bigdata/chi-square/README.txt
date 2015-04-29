Big Data 2015 - A Study of linguistic drift - Chi-Square Distance - Cynthia Oeschger

This MapReduce code computes the chi-square distance between two years.

It takes as arguments :
        - The input directory containg the n-grams for each year
        - The output directory

A call sample :

hadoop jar ChiSquare.jar ch/epfl/bigdata/ChiSquare /projects/linguistic-shift/corrected_ngrams/1-grams output


IMPORTANT : These files must exist in order to run the application :
        - /projects/linguistic-shift/stats/1-grams-TotOccurenceYear/YearOccurences
        - /projects/linguistic-shift/stats/WordOccurenceOverAllYears
