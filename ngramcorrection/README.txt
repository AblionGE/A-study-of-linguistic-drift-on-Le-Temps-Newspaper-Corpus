Big Data 2015 - A Study of linguistic drift - OCR Error Correction - Tao Lin

This MapReduce code corrects simple ocr error.

It takes as arguments :
        - The input directory containg the n-grams for each year
        - The output directory

A call sample :

hadoop jar ocrcorrection.jar ch.epfl.bigdata.linguisticdrift.errorcorrection.Driver /projects/linguistic-shift/ngrams/1-grams /projects/linguistic-shift/corrected_ngrams/1-grams