/*
 * Big Data 2015 - A Study of linguistic drift - Select a subset of articles in a year
 * Example of use
 * spark-submit --class "SelectArticle" --master yarn-cluster --executor-memory 8g --num-executors 50 target/scala-2.10/selectarticle_2.10-1.0.jar 1995 1000 hdfs:///projects/linguistic-shift/corrected_nGramArticle/ hdfs:///user/your_username/
 */

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object SelectArticle {
  def main(args: Array[String]) {

    if (args.size != 4) {
        println("Use with 4 args : the year of articles, number of articles, directory of articles, output directory")
        exit(1)
    }

    val sc = new SparkContext(new SparkConf().setAppName("SelectArticle"))

    val articles = sc.textFile(args(2) + "/" + args(0) + "*")
    val articles_temp = articles.map(e => e.split(", ")).map(e => e.flatMap(f => f.split('\t'))).groupBy(e => e(2)).map(e => e._2.toArray)
    val sample_article = sc.parallelize(articles_temp.takeSample(false, args(1).toInt, scala.util.Random.nextInt(10000)))
    val formatted_article = sample_article.flatMap(e => e.map(f => (f(0), f(1).toInt))).reduceByKey(_+_)
    
    val general_formatted_article = formatted_article.map(e => e._1 + "\t" + e._2)
    general_formatted_article.saveAsTextFile(args(3))
    
    sc.stop()
  }
}