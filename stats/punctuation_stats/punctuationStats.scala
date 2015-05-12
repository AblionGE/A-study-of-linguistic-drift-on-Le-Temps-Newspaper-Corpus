/*
 *  Big Data 2015 - A Study of linguistic drift - Compute means of sentences lengths
 *  Author : Gil Brechbühler (gbrechbu)
 */

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object PunctuationStats {

  def mapSum(e: (String, String)) : (String, Array[Int], Int) = {
    val sentenceSplit = e._2.split("[.?!]")
    val toRet = (e._1, Array(sentenceSplit.map(f => f.count(_ == ',')).reduce(_+_), sentenceSplit.map(f => f.count(_ == ';')).reduce(_+_), 
        sentenceSplit.map(f => f.count(_ == ':')).reduce(_+_)), sentenceSplit.length)
    return toRet;
  }

  def main(args: Array[String]) {

    val sc = new SparkContext(new SparkConf().setAppName("Punctuation statistics"))

    val linesByYearTemp = sc.wholeTextFiles("hdfs:///projects/linguistic-shift/articles_separated/*")
    //val linesByYear = linesByYearTemp.map(e => (e._1.substring(e._1.size - 4), e._2.replaceAll("<full_text>|</full_text>", "").replaceAll("[^a-zA-ZÀÂÄÈÉÊËÎÏÔŒÙÛÜŸàâäèêéëîïôœùûüÿÇç.?!,:;]", " ").replaceAll("[A-Z]{1,3}[a-z]{0,2} \\.|[A-Z]{1,3}[a-z]{0,2}\\.", "a")))

    val temp1 = linesByYearTemp.map(e => (e._1.substring(e._1.size - 4), e._2.replaceAll("<full_text>|</full_text>", "")))
    val temp2 = temp1.map(e => (e._1, e._2.replaceAll("[^a-zA-ZÀÂÄÈÉÊËÎÏÔŒÙÛÜŸàâäèêéëîïôœùûüÿÇç.?!,:;]", " ")))
    val linesByYear = temp2.map(e => (e._1, e._2.replaceAll("[A-Z]{1,2}[a-z]{0,3}[ ]?[.]", "a")))

    val sumPunctuation = linesByYear.map(e => mapSum(e))

    val meanPunctuation = sumPunctuation.map(e => (e._1, e._2.map(f => f.toFloat / e._3.toFloat), e._3)).sortBy(_._1)

    val toOutput = meanPunctuation.map(e => e._1 + "," + e._2.mkString(","))

    toOutput.saveAsTextFile("hdfs:///projects/linguistic-shift/stats/punctuationStats/means")

    sc.stop()
  }
}