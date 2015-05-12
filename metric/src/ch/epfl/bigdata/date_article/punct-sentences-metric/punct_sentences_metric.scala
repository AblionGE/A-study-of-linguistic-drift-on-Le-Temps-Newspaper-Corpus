/*
 *  Big Data 2015 - A Study of linguistic drift - Punctuation and sentences statistics metrc - Gil Brechbühler
 */

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.collection.mutable.ArrayBuffer

/*
 * Takes a set of articles from one year and computes the distance to every year to try to date this set of articles.
 * Output : a file in format : year_of_sample,year,distance
 */
object PunctSentencesMetric {

  /*
   * Simply computes the distance by year.
   * yearsSentenLengths : the average length of sentences by year. Format of one element: (year, [average_length])
   * yearStats : the punctuation statistics for each year. Format of one element : (year, [average_comas, average_semicolons, average_colons]).
   *    average_xxx is an average by sentence for the corresponding year.
   * statsArts : the statistics of the articles we want to date. Format : [average_sentence_length,  average_comas, average_semicolons, average_colons]
   * initString : the year of the articles to date.
   */
  def distanceByYear(yearsSentenLengths: Array[(String, Array[Float])], yearsStats: Array[(String, Array[Float])], statsArts: Array[Float], initString: String) : Array[(String, Float)] = {
    var res = ArrayBuffer[(String, Float)]()
    for (i <- 0 to (1998-1840-1)) {
        val sentenLengthDist = math.pow(math.abs(statsArts(0) - yearsSentenLengths(i)._2(0)), 2)
        val comasDist = math.pow(math.abs(statsArts(1) - yearsStats(i)._2(0)), 2)
        val semicolonDist = math.pow(math.abs(statsArts(2) - yearsStats(i)._2(1)), 2)
        val colonDist = math.pow(math.abs(statsArts(3) - yearsStats(i)._2(2)), 2)
        val dist = math.sqrt(sentenLengthDist + comasDist + semicolonDist + colonDist)
        val str = initString + "," + (1840 + i).toString
        res += ((str, dist.toFloat))
    }

    return res.toArray;
  }
  
  /*
   * Takes an article and computes some sums, output : ([sum_of_sentences_lengths, sum_of_comas, sum_of_semicolons, sum_of_colons], number_of_sentences)
   */
  def mapSum(article: String) : (Array[Int], Int)  = {
    val sentenceSplit = article.split("[.?!]")
    val toRet = (Array(sentenceSplit.map(f => f.split("\\s+").length).reduce(_+_), sentenceSplit.map(f => f.count(_ == ',')).reduce(_+_), sentenceSplit.map(f => f.count(_ == ';')).reduce(_+_), 
        sentenceSplit.map(f => f.count(_ == ':')).reduce(_+_)), sentenceSplit.length)
    return toRet;
  }

  def main(args: Array[String]) {

    if (args.size != 3) {
        println("Use with 3 args : year, number of articles, output directory")
        exit(1)
    }

    val sc = new SparkContext(new SparkConf().setAppName("Punctuation-Sentences-Metric"))

    val meanSentenLengths = sc.textFile("hdfs:///projects/linguistic-shift/stats/sentencesLength/means.csv")
    val punctStats = sc.textFile("hdfs:///projects/linguistic-shift/stats/punctuationStats/stats.csv")

    val articlesTemp = sc.textFile("hdfs:///projects/linguistic-shift/articles_separated/" + args(0))
    val articles = articlesTemp.map(e => e.replaceAll("<full_text>|</full_text>", ""))

    val sampleArticles = sc.parallelize(articles.takeSample(false, args(1).toInt, scala.util.Random.nextInt(10000)))

    val statsArticlesTemp = sampleArticles.map(e => mapSum(e))
    val statsArticles = statsArticlesTemp.map(e => e._1.map(f => f.toFloat / e._2.toFloat))

    // Sum the means for articles to be able to compute a mean for the full sample.
    val statsArticlesGlobTemp = statsArticles.collect.foldLeft(Array(0.0f,0.0f,0.0f,0.0f))((a: Array[Float], b: Array[Float]) => Array(a(0) + b(0), a(1) + b(1), a(2) + b(2), a(3) + b(3)))
    val statsArticlesGlob = statsArticlesGlobTemp.map(f => f / args(1).toFloat)

    // Simple format conversion
    val msl = meanSentenLengths.map(f => f.split(",")).map(f => (f(0), Array(f(1).toFloat)))
    val ps = punctStats.map(f => f.split(",")).map(f => (f(0), Array(f(1).toFloat, f(2).toFloat, f(3).toFloat)))

    val res = distanceByYear(msl.collect, ps.collect, statsArticlesGlob, args(0))

    // Normalization and output
    val biggestDist = res.sortBy(_._2).last._2

    val toOutput = sc.parallelize(res.map(e => e._1 + "," + (e._2 / biggestDist).toString))

    toOutput.saveAsTextFile(args(2))

    sc.stop()
  }
}