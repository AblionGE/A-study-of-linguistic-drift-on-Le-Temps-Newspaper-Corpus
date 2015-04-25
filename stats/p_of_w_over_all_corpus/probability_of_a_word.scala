/*
 *  Big Data 2015 - A Study of linguistic drift - Probability to have a Word w in all the corpus
 *
 * Example of call :
 * spark-submit --class "ProbabilityOfAWordInAllCorpus" --master yarn-cluster --executor-memory 8g --num-executors 50 \
 * target/scala-2.10/probabilityofawordinallcorpus_2.10-1.0.jar \
 * 1 hdfs:///projects/linguistic-shift/corrected_ngrams/1-grams/ hdfs:///user/maschaer/outputTest/ 2>err
 */

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object ProbabilityOfAWordInAllCorpus {
  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("ProbabilityOfAWordInAllCorpus"))

    if (args.size != 3) {
        // the input format is important to parse the data because they are not the same if the input file
        // was create with MapReduce or with Spark
        println("Use with 3 args : nbOfGrams, input directory, output directory")
        exit(1)
    }

    val nbOfGrams = args(0)

    // Read all files
    val wordsFile = args(1)

    val words = sc.textFile(wordsFile)

    val total_words = words.flatMap(e => e.split(", ").map(f => f.split('\t')).map(e => (1, e(1).toDouble))).reduceByKey(_+_).map(e => e._2).collect
    //val total_words = YearOccurrences.map(e => e.split("\t")(1)).map(e => (1,e.toDouble)).groupBy(e => e._1).map(e => e._2.map(f => f._2).toList).map(e => e.sum).collect

    val occurrences_per_words = words.map(e => e.split("\t")).groupBy(e => e(0)).map(e => (e._1, (e._2.toArray.map(f => f(1).toDouble)).sum))

    val results = occurrences_per_words.map(e => (e._1, e._2/total_words(0)))

    results.saveAsTextFile(args(2))

    sc.stop()
  }
}