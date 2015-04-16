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
     * Help function for compute kl distance for one word
     * Take a word of one year and the liste of this word for each year and compute all combinations of them
     */
    def compute_kl_one_word_help(w: List[String], l: List[List[String]]) : List[(String, String)] = l match {
      case List() => List()
      case _ if (w.tail.head.toDouble > 0.0) => ((w.tail.head.toDouble * 
        Math.log(l.head.tail.head.toDouble / w.tail.head.toDouble)).toString, w.tail.tail.head + "-" + l.head.tail.tail.head) :: compute_kl_one_word_help(w, l.tail)
      case _ => ("0.0", w.tail.tail.head + "-" + l.head.tail.tail.head) :: compute_kl_one_word_help(w, l.tail)
    }
    
    /**
     * Compute the Kullback-Leibler distance
     * Arg : list of one word for all years
     */
    def compute_kl_one_word(w: List[List[String]]) : List[List[(String, String)]] = {
      w.map(e => compute_kl_one_word_help(e, w))
    }

    // Read all files
    val lines = sc.wholeTextFiles("/home/marc/temp/test*")
    //val lines = sc.wholeTextFiles("/home/marc/temp/1*-r-*")
    //val lines = sc.wholeTextFiles("hdfs:///user/maschaer/input/*")

    // format all triplets as a List containing word, value, year
    //val all_triplets = lines.map(el => el._2.split('\n').map(t => t.split(' ').toList).map(t => t ++ List(el._1.split("-r-")(0)))).flatMap(e => e)
    val all_triplets = lines.map(el => el._2.split('\n').map(t => t.split(' ').toList).map(t => t ++ List(el._1.split("test")(1)))).flatMap(e => e)

    val grouped_and_ordered = all_triplets.groupBy(e => e.head).map(e => e._2.toList).map(e => e.sortBy(f => f.tail.tail.head))

    val completed = grouped_and_ordered.map(e => add_missed_word(e, e.head.head, 1, 2))

    val vectors_of_values = completed.flatMap(compute_kl_one_word).flatMap(e => e).groupBy(e => e._2).map(e => (e._1, e._2.toList)).map(e => (e._1, e._2.map(f => f._1.toDouble)))

    val results = vectors_of_values.map(e => (e._1, -e._2.sum)).sortBy(e => e._1)
    results.saveAsTextFile("/home/marc/temp/results")


    sc.stop()
  }
}