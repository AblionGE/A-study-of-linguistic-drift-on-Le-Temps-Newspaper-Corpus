/*
 *  Big Data 2015 - A Study of linguistic drift - Date a set of article with Kullback-Leibler Divergence
 * Example of use
 * spark-submit --class "KullbackLeiblerArticle" --master yarn-cluster --executor-memory 8g --num-executors 100 target/scala-2.10/kullback-leibler_2.10-1.0.jar 1 hdfs:///projects/linguistic-shift/stats/Corrected/ProbabilityOfAWordOverAllYears/1-grams/ hdfs:///projects/linguistic-shift/stats/Corrected/ProbabilityOfAWordPerYear/1-grams "Spark" hdfs:///user/maschaer/out/ "1997" 30 hdfs:///projects/linguistic-shift/corrected_nGramArticle/ 2>err
 * nbOfGrams : 1 to 3
 * directory of articles : (corrected_)nGramArticle/nGram
 */

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object SelectArticle {
  def main(args: Array[String]) {

    if (args.size != 4) {
        // the input format is important to parse the data because they are not the same if the input file
        // was create with MapReduce or with Spark
        println("Use with 4 args : the year of articles, number of articles, directory of articles, output directory")
        exit(1)
    }

    val sc = new SparkContext(new SparkConf().setAppName("SelectArticle"))

    val articles = sc.textFile(args(2) + "/" + args(0) + "*")
    val articles_temp = articles.map(e => e.split(", ")).map(e => e.flatMap(f => f.split('\t'))).groupBy(e => e(2)).map(e => e._2.toArray)
    val sample_article = sc.parallelize(articles_temp.takeSample(true, args(1).toInt, scala.util.Random.nextInt(1000)))
    val formatted_article = sample_article.flatMap(e => e.map(f => (f(0), f(1)))).reduceByKey(_+_)
    
    formatted_article.saveAsTextFile(args(3))
    
    sc.stop()
  }
}