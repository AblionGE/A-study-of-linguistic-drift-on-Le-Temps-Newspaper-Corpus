/*
 * Big Data 2015 - A Study of linguistic drift - Probability to have a Word w in all the corpus - Marc Schaer
 *
 * Example of call :
 * spark-submit --class "ProbabilityOfAWordInAllCorpus" --master yarn-cluster --executor-memory 8g --num-executors 50 \
 * target/scala-2.10/probabilityofawordinallcorpus_2.10-1.0.jar \
 * hdfs:///projects/linguistic-shift/corrected_ngrams/1-grams/ hdfs:///user/maschaer/outputTest/ 2>err
 */

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object ProbabilityOfAWordInAllCorpus {
  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("ProbabilityOfAWordInAllCorpus"))

    if (args.size != 2) {
        println("Use with 2 args : input directory, output directory")
        exit(1)
    }

    // Read all files
    val wordsFile = args(0) + "/*"
    val words = sc.textFile(wordsFile)

    val total_words = words.flatMap(e => e.split(", ").map(f => f.split('\t')).map(e => (1, e(1).toDouble))).reduceByKey(_+_).map(e => e._2).collect
    val occurrences_per_words = words.flatMap(e => e.split(", ").map(f => f.split('\t'))).groupBy(e => e(0)).map(e => (e._1, (e._2.toArray.map(f => f(1).toDouble)).sum))

    val results = occurrences_per_words.map(e => (e._1, e._2/total_words(0)))
    val results_formatted = results.map(e => e._1 + "\t" + e._2)
    results_formatted.saveAsTextFile(args(1))

    sc.stop()
  }
}