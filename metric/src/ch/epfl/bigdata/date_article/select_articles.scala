/*
 *  Big Data 2015 - A Study of linguistic drift - Select a subset of articles in a year
 * Example of use
 * 
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