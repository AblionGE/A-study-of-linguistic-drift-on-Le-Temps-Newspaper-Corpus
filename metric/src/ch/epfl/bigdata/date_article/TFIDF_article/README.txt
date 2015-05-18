Big Data 2015 - A Study of linguistic drift - Compute the TF-IDF values for articles - Marc Schaer

This code computes the TF-IDF values for an article.
It takes as arguments :
        - A directory containing articles
        - An output directory
        - The directory with the whole corpus

A call sample (You have to be in the directory where the code is and you have to compile it with the compile script) :

hadoop jar TFIDF/TFIDF.jar ch.bigdata2015.linguisticdrift.tfidf.TFIDF /projects/linguistic-shift/articles_samples/15_articles/Corrected/1995 output_directory /projects/linguistic-shift/corrected_ngrams/1-grams

You have to assume that the articles directory exists and contains files and that the files starts with the year of the article for the 4th first letters of the filename.

The different directories for the articles are in /projects/linguistic-shift/articles_samples/ or you can take a subset of article with the select_article program.
