/*
 *  Big Data 2015 - A Study of linguistic drift - Kullback-Leibler Metric
 */

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object KullbackLeibler {
  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("Kullback-Leibler"))

    val nbOfGrams = "1"

    // Read all files
    //val file = "/home/marc/temp/19*"
    //val probabilityOfAWordFile = "/home/marc/temp/proba/*"
    val probabilityOfAWordFile = "hdfs:///projects/linguistic-shift/stats/ProbabilityOfAWord/" + nbOfGrams + "-grams/*"
    val file = "hdfs:///projects/linguistic-shift/tfidf/" + nbOfGrams + "-grams/*"
    val splitter = file.split('/').size
    val lines = sc.wholeTextFiles(file)
    val probabilityOfAWord = sc.textFile(probabilityOfAWordFile)
    val probabilityOfAWordTemp = probabilityOfAWord.map(e => e.split('(')(1).split(')')(0).split(',')).map(e => (e(0), e(1), "0000"))

    /**
     * This function takes a List of List of String where the inner list contains 3 elements : a word, a value and a year.
     * The year arg must be for the call the oldest year and maxYear the youngest and the word is the word contained
     * the list.
     * This function adds missing tuple for years where the word doesn't appear
     */
    def add_missed_word(l: List[(String, String, String)], word: String, year: Integer, maxYear: Integer) : List[(String, String, String)] = l match {

      case List() if (year > maxYear) => List()
      case notEmpty if (!l.isEmpty) => notEmpty match {
        case _ if (l.head._3 == "0000") => l.head :: add_missed_word(l.tail, word, year, maxYear)
        case _ if (l.head._3.toInt == year) => l.head :: add_missed_word(l.tail, word, year+1, maxYear)
        case _ => (word, "0.0", year.toString) :: add_missed_word(l, word, year+1, maxYear)
      }
      case _ => (word, "0.0", year.toString) :: add_missed_word(l, word, year+1, maxYear)
    }

    val mu = 1E-25

    /*
     * Help function for compute kl distance for one word
     * Take a word of one year and the liste of this word for each year and compute all combinations of them
     */
    def compute_kl_one_word_help(w: (String, String, String), l: List[(String, String, String)], proba: Double) : List[(String, String)] = l match {
      case List() => List()
      case _ if (w._2.toDouble == l.head._2.toDouble) => compute_kl_one_word_help(w, l.tail, proba)
      case _ => (((w._2.toDouble + mu*proba) * 
        Math.log((w._2.toDouble + mu*proba) / (l.head._2.toDouble + mu*proba))).toString, w._3 + ":" + l.head._3) :: compute_kl_one_word_help(w, l.tail, proba)
    }
    
    /**
     * Compute the Kullback-Leibler distance
     * Args : list of one word for all years and the probability to have this word over all years
     */
    def compute_kl_one_word(w: List[(String, String, String)], proba: Double) : List[List[(String, String)]] = {
      w.map(e => compute_kl_one_word_help(e, w, proba))
    }

    /**
     *  Create a list of pairs for distance between the same year (useful for plotting)
     * Args : the min year and the max year
     */
    def create_identity_distances(startYear: Integer, maxYear: Integer) : List[(String, Double)] = startYear match {
      case _ if (startYear > maxYear) => List()
      case other => (startYear.toString + ":" + startYear.toString, 0.0) :: create_identity_distances(startYear+1, maxYear)
    }

    // format all triplets as a List containing value, word, year
    val all_triplets = lines.map(el => el._2.split('\n').map(t => t.split(' ').toList).map(t => t ++ List(el._1.split("-r-")(0).split('/')(splitter-1)))).flatMap(e => e)

    val grouped_and_ordered_temp = all_triplets.union(probabilityOfAWordTemp.map(e => List(e._1, e._2, "0000"))).groupBy(e => e.head)

    val grouped_and_ordered = grouped_and_ordered_temp.map(e => e._2.toList.map(f => (f.head, f.tail.head, f.tail.tail.head))).map(e => e.sortBy(f => f._3))

    val completed = grouped_and_ordered.map(e => add_missed_word(e, e.head._1, 1840, 1998))

    val vectors_of_values_temp = completed.flatMap(e => compute_kl_one_word(e.tail, e.head._2.toDouble)).flatMap(e => e)

    val vectors_of_values = vectors_of_values_temp.map(e => (e._2, e._1.toDouble))

    val results_temp = vectors_of_values.reduceByKey(_+_).union(sc.parallelize(create_identity_distances(1840, 1998)))

    val results = results_temp.sortBy(e => e._1)

    results.saveAsTextFile("hdfs:///projects/linguistic-shift/Kullback-Leibler/" + nbOfGrams + "-grams")
    //results.saveAsTextFile("/home/marc/temp/results")


    sc.stop()
  }
}