/*
 * Big Data 2015 - A Study of linguistic drift - Select a subset of articles in a year
 */

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object SelectArticle {
  def main(args: Array[String]) {

    if (args.size != 5) {
        println("Use with 5 args : the year of articles, number of articles, directory of articles, output directory, seed for random")
        exit(1)
    }

    val sc = new SparkContext(new SparkConf().setAppName("SelectArticle"))
    
    val articles = sc.textFile(args(2) + "/" + args(0) + "*")
    val articlesTemp = articles.map(e => e.split(", ")).map(e => e.flatMap(f => f.split('\t'))).groupBy(e => e(2)).map(e => e._2.toArray)
    val sampleArticle = sc.parallelize(articlesTemp.takeSample(false, args(1).toInt, args(4).toInt))
    val formattedArticle = sampleArticle.flatMap(e => e.map(f => (f(0), f(1).toInt))).reduceByKey(_+_)
    
    val generalFormattedArticle = formattedArticle.map(e => e._1 + "\t" + e._2)
    generalFormattedArticle.saveAsTextFile(args(3))
    
    sc.stop()
  }
}
