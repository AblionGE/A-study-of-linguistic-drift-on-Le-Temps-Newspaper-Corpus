Big Data 2015 - A Study of linguistic drift - Creation of the topic clustering - jweber & fbouassid

- INFORMATION: 
NGramTopic is seperate into 3 different step : 
First: Ngram which output a article with their word and the occurence of a word. We can then use this output to make some OCR correction. Then we run WordArticle
which combine for every article all the words.

Second: SparkTopicClustering/TopicClustering.scala creates the topics given the words by article.

Third: ComputeArticleTopic which output for a specific topic all the year with the different words about this topic

- LAUNCH:
Ngram:
Input Arguments:
	- The folder containing the text from the newspaper
	- The output directory
Call sample: 
hadoop jar Ngram.jar Ngram /projects/dh-shared outputnGramNonCorrected

WordArticle: 
Input Arguments:
	- The precedent output directory as input (outputnGramNonCorrected) or the one that have been corrected (/projects/linguistic-shift/corrected_nGramArticle/nGramArticle)
	- The output directory 

hadoop jar Ngram.jar WordArticle /projects/linguistic-shift/corrected_nGramArticle/nGramArticle ArticleIDToWord

The output follows this format:
articleID<TAB>word<TAB>occurence,articleID<TAB>word2<TAB>occurence2,...

TopicClustering:
Input Arguments:
	- input directory from before (ArticleIDToWord)
	- number of cluster which has to be equal to 15. Otherwise the parser has to be modified
Output: 
	- This gives us three output: Result, Results/ParsedResult and Results/gramOcc. We only consider ParsedResult and gramOcc.
	  In ParsedResult we have the link between an articleID and the topic which it belongs to.
	  In gramOcc we have the link between an articleID and the words and their occurence in this article
	 
Call Sample:
spark-submit --class TermIndexing --master yarn-client --executor-memory 10g --driver-memory 10g --num-executors 100 topicclustering_2.10-1.0.jar "hdfs:///projects/linguistic-shift/corrected_nGramArticle/nGramArticle/*" 15
ComputeArticleTopic:
Input Arguments:
	- Results/ParsedResult
	- Results/gramOcc
	- output Directory
Call Sample:
hadoop jar ComputeArticleTopic.jar ComputeArticleTopic Results/ParsedResult Results/gramOcc articleByTopNum





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