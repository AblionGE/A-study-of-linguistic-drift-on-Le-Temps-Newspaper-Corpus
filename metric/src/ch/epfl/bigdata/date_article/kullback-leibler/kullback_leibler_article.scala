/*
 * Big Data 2015 - A Study of linguistic drift - Date a set of article with Kullback-Leibler Divergence - Marc Schaer
 */

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object KullbackLeiblerArticle {
  def main(args: Array[String]) {

    if (args.size != 5) {
        println("Use with 5 args : nbOfGrams, \"Corrected\" or \"WithoutCorrection\", output directory, directory of articles, year of articles")
        exit(1)
    }

    if (args(1) != "Corrected" && args(1) != "WithoutCorrection") {
        println("Use with 5 args : nbOfGrams, \"Corrected\" or \"WithoutCorrection\", output directory, directory of articles, year of articles")
        exit(1)
    }

    val sc = new SparkContext(new SparkConf().setAppName("Kullback-Leibler-article"))

    val nbOfGrams = args(0)
    val minYear = 1839
    val maxYear = 1998

    // Read all files
    var probabilityOfAWordFile = ""
    var file = ""
    if (args(1) == "Corrected") {
      probabilityOfAWordFile = "hdfs:///projects/linguistic-shift/stats/Corrected/ProbabilityOfAWordOverAllYears/" + nbOfGrams + "-grams/*"
      file = "hdfs:///projects/linguistic-shift/corrected_ngrams/" + nbOfGrams +"-grams/*"  
    } else {
      probabilityOfAWordFile = "hdfs:///projects/linguistic-shift/stats/WithoutCorrection/ProbabilityOfAWordOverAllYears/" + nbOfGrams + "-grams/*"
      file = "hdfs:///projects/linguistic-shift/ngrams/" + nbOfGrams +"-grams/*"
    }

    val articlesFile = args(3) + "*"

    val splitter = file.split('/').size
    val lines = sc.wholeTextFiles(file)
    val probabilityOfAWord = sc.textFile(probabilityOfAWordFile)
    val probabilityOfAWordTemp = probabilityOfAWord.map(e => e.split('\t')).map(e => (e(0), e(1), "0000"))

    val articles = sc.textFile(articlesFile)
    val formattedArticles = articles.map(e => e.split('\t')).map(e => (e(0), e(1).toInt, "1839"))
    val totalWordsArticles = formattedArticles.map(e => (1, e._2)).reduceByKey(_+_).collect
    val normalizedArticles = formattedArticles.map(e => (e._1, e._2.toDouble/totalWordsArticles(0)._2.toDouble, e._3))

    /**
     * This function takes a List of List of String where the inner list contains 3 elements : a word, a value and a year.
     * The year arg must be for the call the oldest year and maxYear the youngest and the word is the word contained
     * the list.
     * This function adds missing tuple for years where the word doesn't appear
     */
    def addMissedWords(l: List[(String, Double, String)], word: String, year: Integer, maxYear: Integer) : List[(String, Double, String)] = l match {

      case List() if (year > maxYear) => List()
      case notEmpty if (!l.isEmpty) => notEmpty match {
        case _ if (l.head._3 == "0000") => l.head :: addMissedWords(l.tail, word, year, maxYear)
        case _ if (l.head._3.toInt == year) => l.head :: addMissedWords(l.tail, word, year+1, maxYear)
        case _ => (word, 0.0, year.toString) :: addMissedWords(l, word, year+1, maxYear)
      }
      case _ => (word, 0.0, year.toString) :: addMissedWords(l, word, year+1, maxYear)
    }

    val mu = 1E-25

    /*
     * Help function for compute kl distance for one word
     * Take a word of one year and the liste of this word for each year and compute all combinations of them
     */
    def computeKLOneWordHelp(w: (String, Double, String), l: List[(String, Double, String)], proba: Double) : List[(String, String)] = l match {
      case List() => List()
      case _ if (w._2.toDouble == l.head._2.toDouble) => computeKLOneWordHelp(w, l.tail, proba)
      case _ => (((w._2.toDouble + mu*proba) * 
        Math.log((w._2.toDouble + mu*proba) / (l.head._2.toDouble + mu*proba))).toString, w._3 + "," + l.head._3) :: computeKLOneWordHelp(w, l.tail, proba)
    }
    
    /**
     * Compute the Kullback-Leibler distance
     * Args : list of one word for all years and the probability to have this word over all years
     */
    def computeKLOneWord(w: List[(String, Double, String)], proba: Double) : List[List[(String, String)]] = {
      //w.map(e => if (e._3 == "1839") computeKLOneWordHelp(e, w, proba) else computeKLOneWordHelp(e, w.filter(f => f._3 == "1839"), proba))
      w.map(e => if (e._3 == "1839") computeKLOneWordHelp(e, w, proba) else computeKLOneWordHelp(e, List(), proba))
    }

    /**
     * Create a list of pairs for distance between the same year (useful for plotting)
     * Args : the min year and the max year
     */
    def createIdentityDistances(startYear: Integer, maxYear: Integer) : List[(String, Double)] = startYear match {
      case _ if (startYear > maxYear) => List()
      case other => (startYear.toString + "," + startYear.toString, 0.0) :: createIdentityDistances(startYear+1, maxYear)
    }

    /**
     * Compute the probability of each word in a year
     * Arg : List of word from a year (word, number of occurrences, year)
     */
    def computeOneYearProbability(list: List[(String, Int, String)]) : List[(String, Double, String)] = {
      val total_words = list.map(e => (1, e._2.toInt)).foldLeft(0)((acc,curr) => acc+curr._2)
      list.map(e => (e._1, e._2.toDouble/total_words, e._3))
    }

    // format all triplets as a List containing word, nb_of_occurrences, year
    val allTriplets = lines.map(el => el._2.split('\n').map(t => t.split('\t').toList).map(t => t ++ List(el._1.split("-r-")(0).split('/')(splitter-1)))).flatMap(e => e).groupBy(e => e.head).flatMap(e => e._2.toList).map(e => (e.head, e.tail.head.toInt, e.tail.tail.head))

    //Remove words from articles
    val removedWords = allTriplets.filter(e => e._3 == args(4)).union(formattedArticles).groupBy(e => e._1).map(e => e._2).flatMap(e => e.map(f => (f._1, f._2))).reduceByKey(_-_).map(e => (e._1, e._2, args(4)))
    val otherWords = allTriplets.filter(e => e._3 != args(4));
    val newWords = removedWords.map(e => if (e._2 < 0) (e._1, -e._2, e._3) else e).filter(e => e._2 != 0).union(otherWords).groupBy(e => e._3).map(e => e._2.toList)
    val allTripletsFormatted = newWords.flatMap(e => computeOneYearProbability(e))


    val groupedAndOrderedTemp = allTripletsFormatted.union(probabilityOfAWordTemp.map(e => (e._1, e._2.toDouble, "0000"))).union(normalizedArticles).groupBy(e => e._1)
    val groupedAndOrdered = groupedAndOrderedTemp.map(e => e._2.toList).map(e => e.sortBy(f => f._3))

    // Add missed words in each year
    val completed = groupedAndOrdered.map(e => addMissedWords(e, e.head._1, minYear, maxYear))

    // Get a List of List of pairs (value, year1:year2)
    val vectorsOfValuesTemp = completed.flatMap(e => computeKLOneWord(e.tail, e.head._2.toDouble)).flatMap(e => e)
    // Get the same List as before but with inversion of year1:year2 and value
    val vectorsOfValues = vectorsOfValuesTemp.map(e => (e._2, e._1.toDouble))


    val resultsTemp = vectorsOfValues.reduceByKey(_+_)
    val results = resultsTemp.sortBy(e => e._1).map(e => if (e._2.toDouble <0) (e._1, -e._2.toDouble) else e)

    // Normalization
    val max = results.map(e => e._2.toDouble).top(1)
    val resultsNormalized = results.map(e => (e._1, e._2.toDouble/max(0)))
    val resultsFormatted = resultsNormalized.map(e => e._1 + "," + e._2)
    resultsFormatted.saveAsTextFile(args(2))

    // Get the values where the articles are simulated by years
    /*val resultsNormalizedFromArticle = resultsNormalized.filter(e => e._1.split(',')(0) == "1839")
    val bestEstimationFromArticle = resultsNormalizedFromArticle.map(e => e._2.toDouble).takeOrdered(1)
    val bestEstimationFromArticleWithYear = resultsNormalizedFromArticle.filter(e => e._2.toDouble == bestEstimationFromArticle(0)).map(e => (e._1.split(',')(1), e._2))

    // Get the values where the years arte simulated by articles
    val resultsNormalizedToArticle = resultsNormalized.filter(e => e._1.split(',')(0) != "1839")
    val bestEstimationToArticle = resultsNormalizedToArticle.map(e => e._2.toDouble).takeOrdered(1)
    val bestEstimationToArticle_with_year = resultsNormalizedToArticle.filter(e => e._2.toDouble == bestEstimationToArticle(0)).map(e => (e._1.split(',')(0), e._2))
   
    val combineBestEstimations = bestEstimationFromArticleWithYear.union(bestEstimationToArticle_with_year)
    val combineBestEstimations_formatted = combineBestEstimations.map(e => e._1 + "," + e._2)
    combineBestEstimations_formatted.saveAsTextFile(args(2) + "/best_estimation/")
    */
    //bestEstimationFromArticleWithYear.map(e => e._1 + "," + e._2).saveAsTextFile(args(2) + "/best_estimation/")

    sc.stop()
  }
}