INFORMATION : 
The NGramTopic is used to keep a link between every words with their article.
We re use the Ngram reader and add a few lines in order to have as output : 
word <TAB> occurence <TAB> articleID
This output allows us also to correct the word easily by keeping the same format (word <TAB> occurence)
Next we use WordArticle to link an article with all the words they contain and the occurence of a word.

LAUNCH :
First Launch Ngram.java with Hadoop to obtain the first output. The two arguments of the method are simply all the articles and the output.
hadoop jar Ngram Ngram /projects/dh-shared outputnGramNonCorrected

Second we launch the final step on the corrected words. But we could have launch it over the non corrected
hadoo jar WordArticle WordArticle /projects/linguistic-shift/corrected_nGramArticle/nGram nGramArticleCorrected

Final:
At the end of this procedure we have for every word their respective articleID. This output will be used as input of the 
Spark job TopicClustering