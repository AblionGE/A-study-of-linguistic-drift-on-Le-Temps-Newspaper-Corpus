/*
 *  Big Data 2015 - A Study of linguistic drift - Probability to have a Word w per year
 */

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object ProbabilityOfAWordPerYear {
  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("ProbabilityOfAWordPerYear"))
    
    val nbOfGrams = "1"
    val yearMin = 1840
    val yearMax = 1998

    // Read all files
    val YearOccurrencesFile = "hdfs:///projects/linguistic-shift/stats/" + nbOfGrams + "-grams-Tot*"
    //val YearOccurrencesFile = "/home/marc/temp/1-grams-TotOccurenceYear*"
    val YearOccurrences = sc.textFile(YearOccurrencesFile)

    def compute_one_year(year: Integer) : Int = {
        if (year <= yearMax) {
            val wordsFile = "hdfs:///projects/linguistic-shift/cor_ngrams/" + nbOfGrams + "-grams/" + year +"*"
            //val wordsFile = "/home/marc/temp/" + year +"*"
            val words = sc.textFile(wordsFile)

            val total_words = YearOccurrences.map(e => e.split('\t')).filter(e => e(0).toInt == year).map(e => e(1)).collect

            val results = words.map(e => e.split(',')).map(e => e(0).split('\t')).map(e => (e(1), e(0).toDouble/total_words(0).toDouble))

            results.saveAsTextFile("hdfs:///projects/linguistic-shift/stats/ProbabilityOfAWordPerYear/" + nbOfGrams + "-grams/"+year)
            //results.saveAsTextFile("/home/marc/temp/stats_help/ProbabilityOfAWordPerYear/" + nbOfGrams + "-grams/"+year)

            compute_one_year(year+1)
        } else {
            0
        }
    }

    compute_one_year(yearMin)

    sc.stop()
  }
}