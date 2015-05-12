Big Data 2015 - A Study of linguistic drift - TFIDF - Marc Schär and Jérémy Weber

This MapReduce code computes the the TF-IDF value for each word in each year. It is composed of
several phases that compute temporary files for the succession of MapReduce tasks

It takes as arguments :
        - The input directory containg the n-grams for each year
        - The output directory

A call sample :

hadoop jar TFIDF.jar ch.bigdata2015.linguisticdrift.tfidf.TFIDF /projects/linguistic-shift/corrected_ngrams/1-grams /user/your_username