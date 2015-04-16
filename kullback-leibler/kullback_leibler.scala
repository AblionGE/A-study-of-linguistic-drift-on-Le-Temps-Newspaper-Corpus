/*
 *  Big Data 2015 - A Study of linguistic drift - Kullback-Leibler Metric
 */

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object KullbackLeibler {
  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("Kullback-Leibler"))
    //val lines = sc.textFile("hdfs:///user/maschaer/input/*")

    /**
     * This function takes a List of List of String where the inner list contains 3 elements : a word, a value and a year.
     * The year arg must be for the call the oldest year and maxYear the youngest and the word is the word contained
     * the list.
     * This function adds missing tuple for year where the word doesn't appear
     */
    def add_missed_word(l: List[List[String]], word: String, year: Integer, maxYear: Integer) : List[List[String]] = l match {
      case List() if (year > maxYear) => List()
      case notEmpty if (!l.isEmpty) => notEmpty match {
        case _ if (l.head.tail.tail.head.toInt == year) => l.head :: add_missed_word(l.tail, word, year+1, maxYear)
        case _ => List(word, "0.0", year.toString) :: add_missed_word(l, word, year+1, maxYear)
      }
      case _ => List(word, "0.0", year.toString) :: add_missed_word(l, word, year+1, maxYear)
    }

    /*
     * Help function for computing Kullback-Leibler distance
     */
    def compute_kl_help(l: List[(String, String)]) : Double = {
      val temp = l.sortBy(e => e._2)
      if (temp.head._1.toDouble > 0.0) {
        temp.head._1.toDouble * Math.log(temp.tail.head._1.toDouble / temp.head._1.toDouble)
      }
      else 0.0
    }
    
    /**
     * Compute the Kullback-Leibler distance
     */
    def compute_kl(y1: List[List[String]], y2: List[List[String]]) : Double = {
      val temp_list = y1 ++ y2
      val pair_of_value = temp_list.groupBy(e => e.head).map(e => e._2.toList).map(e => e.map(f => (f.tail.head, f.tail.tail.head)))
      - pair_of_value.map(compute_kl_help).sum
    }

    def go_through_all(l1: List[List[String]], )

    // Read all files
    val lines = sc.wholeTextFiles("/home/marc/temp/test*")
    //val lines = sc.wholeTextFiles("/home/marc/temp/1*-r-*")
    //val lines = sc.wholeTextFiles("hdfs:///user/maschaer/input/*")

    // format all triplets as a List containing word, value, year
    //val all_triplets = lines.map(el => el._2.split('\n').map(t => t.split(' ').toList).map(t => t ++ List(el._1.split("-r-")(0)))).flatMap(e => e)
    val all_triplets = lines.map(el => el._2.split('\n').map(t => t.split(' ').toList).map(t => t ++ List(el._1.split("test")(1)))).flatMap(e => e)

    val grouped_and_ordered = all_triplets.groupBy(e => e.head).map(e => e._2.toList).map(e => e.sortBy(f => f.tail.tail.head))

    val completed = grouped_and_ordered.map(e => add_missed_word(e, e.head.head, 1, 2))

    val grouped_by_year = completed.flatMap(e => e).groupBy(e => e.tail.tail.head).map(e => e._2.toList)

    sc.stop()
  }
}