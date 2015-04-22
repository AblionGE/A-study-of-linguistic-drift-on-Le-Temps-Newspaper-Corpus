/*
 *  Big Data 2015 - A Study of linguistic drift - Probability to have a Word w in all the corpus
 */

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object ProbabilityOfAWordInAllCorpus {
  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("ProbabilityOfAWordInAllCorpus"))

    val nbOfGrams = "1"

    // Read all files
    val YearOccurrencesFile = "hdfs:///projects/linguistic-shift/stats/" + nbOfGrams + "-grams-Tot*"
    val wordsFile = "hdfs:///projects/linguistic-shift/corrected_ngrams/" + nbOfGrams + "-grams/*"

    val YearOccurrences = sc.textFile(YearOccurrencesFile)
    val words = sc.textFile(wordsFile)

    val total_words = YearOccurrences.map(e => e.split("\t")(1)).map(e => (1,e.toDouble)).groupBy(e => e._1).map(e => e._2.map(f => f._2).toList).map(e => e.sum).collect

    val occurrences_per_words = words.map(e => e.split("\t")).groupBy(e => e(0)).map(e => (e._1, (e._2.toArray.map(f => f(1).toDouble)).sum))

    val results = occurrences_per_words.map(e => (e._1, e._2/total_words(0)))

    results.saveAsTextFile("hdfs:///projects/linguistic-shift/stats/ProbabilityOfAWord/" + nbOfGrams + "-grams")

    sc.stop()
  }
}