/*
 * Big Data 2015 - A Study of linguistic drift - Probability to have a Word w per year - Marc Schaer
 *
 * spark-submit --class "ProbabilityOfAWordPerYear" --master yarn-cluster --executor-memory 8g --num-executors 50 \
 * target/scala-2.10/probabilityofawordperyear_2.10-1.0.jar 1 hdfs:///projects/linguistic-shift/ngrams/1-grams/ \
 * hdfs:///projects/linguistic-shift/stats/WithoutCorrection/ProbabilityOfAWordPerYear/1-grams/
 */

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object ProbabilityOfAWordPerYear {
  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("ProbabilityOfAWordPerYear"))
    
    if (args.size != 2) {
        // the input format is important to parse the data because they are not the same if the input file
        // was create with MapReduce or with Spark
        println("Use with 2 args : input directory with \"\\\" at the end of the path, output directory with \"\\\" at the end of the path")
        exit(1)
    }

    val yearMin = 1840
    val yearMax = 1998

    def compute_one_year(year: Integer) : Int = {
        if (year <= yearMax) {
            val wordsFile = args(0) + year + "*"
            val words = sc.textFile(wordsFile)

            val total_words = words.flatMap(e => e.split(", ").map(f => f.split('\t')).map(e => (1, e(1).toDouble))).reduceByKey(_+_).map(e => e._2).collect

            val results = words.map(e => e.split('\t')).map(e => e(0) -> e(1).toDouble/total_words(0))

            results.saveAsTextFile(args(1) + year)

            compute_one_year(year+1)
        } else {
            0
        }
    }

    compute_one_year(yearMin)

    sc.stop()
  }
}