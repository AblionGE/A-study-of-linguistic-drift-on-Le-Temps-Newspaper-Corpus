/*
 *  Big Data 2015 - A Study of linguistic drift - Compute means of sentences lengths
 *  Author : Gil Brechbühler (gbrechbu)
 */

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object SentencesMeans {
  def main(args: Array[String]) {

    val sc = new SparkContext(new SparkConf().setAppName("Sentences Length Mean"))

    val linesByYearTemp = sc.wholeTextFiles("hdfs:///projects/linguistic-shift/stats/sentencesLength/byYear/*")
    val linesByYear = linesByYearTemp.map(e => (e._1.substring(e._1.size - 4), e._2.split('\n').map(f => f.split('\t')).map(g => (g(0), g(1)))))

    // Each tuple of the RDD is (year, Array((product, number of sentences of this size)))
    val productCount = linesByYear.map(e => (e._1, e._2.map(f => (f._1.toInt * f._2.toInt, f._2.toInt))))

    // We sum the elements between them in years
    val sumsByYear = productCount.map(e => (e._1, e._2.foldLeft((0,0))((a,b) => (a._1 + b._1, a._2 + b._2))))

    // And compute the mean by year.
    val meanByYearTemp = sumsByYear.map(e => (e._1, e._2._1.toFloat / e._2._2.toFloat))

    val meanByYear = meanByYearTemp.sortBy(_._1)

    val toOutput = meanByYear.map(e => e._1 + "," + e._2)

    toOutput.saveAsTextFile("hdfs:///projects/linguistic-shift/stats/sentencesLength/means")

    sc.stop()
  }
}