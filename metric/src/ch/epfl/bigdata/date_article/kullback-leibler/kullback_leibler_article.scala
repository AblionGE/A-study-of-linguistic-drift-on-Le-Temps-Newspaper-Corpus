/*
 *  Big Data 2015 - A Study of linguistic drift - Date a set of article with Kullback-Leibler Divergence
 * Example of use
 * spark-submit --class "KullbackLeiblerArticle" --master yarn-cluster --executor-memory 8g --num-executors 100 target/scala-2.10/kullback-leibler_2.10-1.0.jar 1 hdfs:///projects/linguistic-shift/stats/Corrected/ProbabilityOfAWordOverAllYears/1-grams/ hdfs:///projects/linguistic-shift/stats/Corrected/ProbabilityOfAWordPerYear/1-grams "Spark" hdfs:///user/maschaer/out/ "1997" 30 hdfs:///projects/linguistic-shift/corrected_nGramArticle/ 2>err
 * nbOfGrams : 1 to 3
 * directory of articles : (corrected_)nGramArticle/nGram
 */

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object KullbackLeiblerArticle {
  def main(args: Array[String]) {

    if (args.size != 8) {
        // the input format is important to parse the data because they are not the same if the input file
        // was create with MapReduce or with Spark
        println("Use with 8 args : nbOfGrams, ProbabilityOverAllYears directory, input directory, input format (\"Java\" or \"Spark\"), output directory, the year of articles, number of articles, directory of articles")
        exit(1)
    }

    if (args(3) != "Java" && args(3) != "Spark") {
        println("Use with 8 args : nbOfGrams, ProbabilityOverAllYears directory, input directory, input format (\"Java\" or \"Spark\"), output directory, the year of articles, number of articles, directory of articles")
        exit(1)
    }

    val sc = new SparkContext(new SparkConf().setAppName("Kullback-Leibler-article"))

    val nbOfGrams = args(0)
    val minYear = 1839
    val maxYear = 1998

    // Read all files
    val probabilityOfAWordFile = args(1) + "/*"

    val file = args(2) + "/*"

    val articlesFile = args(7) + "/" + args(5) + "*"

    val splitter = file.split('/').size
    val lines = sc.wholeTextFiles(file)
    val probabilityOfAWord = sc.textFile(probabilityOfAWordFile)
    val probabilityOfAWordTemp = probabilityOfAWord.map(e => e.split('(')(1).split(')')(0).split(','))
        .map(e => if (e.size == nbOfGrams.toInt+1) {(e.take(nbOfGrams.toInt).mkString(","), e(nbOfGrams.toInt), "0000")} else {(e(0), e(e.size), "0000")})

    val articles = sc.textFile(articlesFile)
    val articles_temp = articles.map(e => e.split(", ")).map(e => e.flatMap(f => f.split('\t'))).groupBy(e => e(2)).map(e => e._2.toArray)
    val sample_article = sc.parallelize(articles_temp.takeSample(true, args(6).toInt, scala.util.Random.nextInt(1000)))
    val formatted_article = sample_article.flatMap(e => e.map(f => (f(0), f(1)))).reduceByKey(_+_)
    val total_words_article = formatted_article.map(e => (1, e._2)).reduceByKey(_+_).collect
    val normalized_article = formatted_article.map(e => (e._1, e._2.toDouble/total_words_article(0)._2.toDouble, "1839"))

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
        Math.log((w._2.toDouble + mu*proba) / (l.head._2.toDouble + mu*proba))).toString, w._3 + "," + l.head._3) :: compute_kl_one_word_help(w, l.tail, proba)
    }
    
    /**
     * Compute the Kullback-Leibler distance
     * Args : list of one word for all years and the probability to have this word over all years
     */
    def compute_kl_one_word(w: List[(String, String, String)], proba: Double) : List[List[(String, String)]] = {
      //w.map(e => if (e._3 == "1839") compute_kl_one_word_help(e, w, proba) else compute_kl_one_word_help(e, w.filter(f => f._3 == "1839"), proba))
      w.map(e => if (e._3 == "1839") compute_kl_one_word_help(e, w, proba) else compute_kl_one_word_help(e, List(), proba))
    }

    /**
     * Create a list of pairs for distance between the same year (useful for plotting)
     * Args : the min year and the max year
     */
    def create_identity_distances(startYear: Integer, maxYear: Integer) : List[(String, Double)] = startYear match {
      case _ if (startYear > maxYear) => List()
      case other => (startYear.toString + "," + startYear.toString, 0.0) :: create_identity_distances(startYear+1, maxYear)
    }

    // format all triplets as a List containing word, value, year
    var all_triplets : org.apache.spark.rdd.RDD[List[String]] = sc.parallelize(List())

    if (args(3) == "Spark") {
            //Manage files from Spark output
            all_triplets = lines.map(el => el._2.split('\n').map(t => t.split('(')(1).split(')')(0).split(',') ++ List(el._1)
                .map(l => l.split("-grams/")(1).split('/')(0)))
                .map(e => Array(e.take(nbOfGrams.toInt).mkString(","), e(nbOfGrams.toInt), e(nbOfGrams.toInt+1)))).flatMap(e => e).map(e => e.toList)
    } else {
            //Manage files from MapReduce Java output
            all_triplets = lines.map(el => el._2.split('\n').map(t => t.split(' ').toList).map(t => t ++ List(el._1.split("-r-")(0).split('/')(splitter-1)))).flatMap(e => e)
    }

    val grouped_and_ordered_temp = all_triplets.union(probabilityOfAWordTemp.map(e => List(e._1, e._2, "0000"))).union(normalized_article.map(e => List(e._1, e._2+"", e._3))).groupBy(e => e.head)

    val grouped_and_ordered = grouped_and_ordered_temp.map(e => e._2.toList.map(f => (f.head, f.tail.head, f.tail.tail.head))).map(e => e.sortBy(f => f._3))

    // Add missed words in each year
    val completed = grouped_and_ordered.map(e => add_missed_word(e, e.head._1, minYear, maxYear))

    // Get a List of List of pairs (value, year1:year2)
    val vectors_of_values_temp = completed.flatMap(e => compute_kl_one_word(e.tail, e.head._2.toDouble)).flatMap(e => e)

    // Get the same List as before but with inversion of year1:year2 and value
    val vectors_of_values = vectors_of_values_temp.map(e => (e._2, e._1.toDouble))

    val results_temp = vectors_of_values.reduceByKey(_+_)//.union(sc.parallelize(create_identity_distances(minYear, maxYear)))

    val results = results_temp.sortBy(e => e._1).map(e => if (e._2.toDouble <0) (e._1, -e._2.toDouble) else e)

    //Normalization
    val max = results.map(e => e._2.toDouble).top(1)

    val results_normalized = results.map(e => (e._1, e._2.toDouble/max(0)))

    results_normalized.saveAsTextFile(args(4))

    sc.stop()
  }
}