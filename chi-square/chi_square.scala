/*
 *  Big Data 2015 - A Study of linguistic drift - Chi-Square Metric
 */

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object ChiSquare {
  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf()
    							.setAppName("Chi-Square")
    							.set("spark.driver.memory", "8g")
    							.set("spark.executor.memory", "8g"))
    
    
     // Read all files
    val files = "hdfs:///user/oeschger/input/*"
    //val file = "hdfs:///projects/linguistic-shift/cor_ngrams/1-grams/*"
    val splitter = files.split('/').size
    val lines = sc.wholeTextFiles(files)

    // List containing tupples (year:Int, word:String, frequency:Double)
    val all_triplets = lines.flatMap(text => text._2.split('\n').map(line => line.split('\t').toList ++ List(text._1.split("-r-")(0).split('/')(splitter-1)))).map(tupple => tupple match {
    	case List(freq, word, year) => (year.toInt, word, freq.toDouble)
    }).cache
    
    // (year, Iterable(year, word, frequency))
    val grouped_by_year = all_triplets.groupBy(el => el._1)
    
    // (word, Iterable(year, word, frequency))
    val grouped_by_words = all_triplets.groupBy(el => el._2)
    
    // Tupples (year, number of words)
    val words_per_year = grouped_by_year.mapValues(el => el.foldLeft(0.0)((acc,num) => acc + num._3)).collect.toList
    
    // Tupples (word, frequency over all years)
    val overall_word_frequency = grouped_by_words.mapValues(el => el.foldLeft(0.0)((acc,num) => acc + num._3)).collect.toList
    
    
    
    
		def get_frequency1(year: Iterable[(Int, String, Double)], word: String): Double = year.find(tupple => tupple._2 == word) match {
			case Some((y, w, freq)) => freq
			case None => 0
		}
    
    def get_frequency2(tupples: List[(Any, Double)], key:Any): Double = tupples.find(t => t._1 == key) match {
    	case Some((key, freq)) => freq
    	case None => 0
    }
    
    def chi_square(year1:(Int, Iterable[(Int, String, Double)]), year2:(Int, Iterable[(Int, String, Double)])): Double = overall_word_frequency.map(word => math.pow((get_frequency1(year1._2,word._1)/get_frequency2(words_per_year,year1._1) - get_frequency1(year2._2,word._1)/word._2),2) / get_frequency2(overall_word_frequency, word._1)).foldLeft(0.0)((acc,num) => acc + num)
    
    
    val result = grouped_by_year.cartesian(grouped_by_year).filter{case (a, b) => a._1 < b._1}.map{case (year1, year2) => (year1._1, year2._1, chi_square(year1, year2))}

    result.saveAsTextFile("hdfs:///user/oeschger/output")


    sc.stop()
  }
}
