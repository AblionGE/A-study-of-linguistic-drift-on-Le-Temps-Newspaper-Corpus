Big Data 2015 - A Study of linguistic drift - Creation of the topic clustering - jweber & fbouassid

- INFORMATION: 
NGramTopic is performed by 3 different steps : 
First: Ngram which output an article with a unique article ID in the format year//uniqueId appended to the words and their occurences. We can then use this output to make some OCR correction. Then we run WordArticle which combine for every article all the words. The Ngram is different from the Ngram to which we apply directly the metrics as we need to keep track of the article Id during the whole process to classify it by topic later.

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

