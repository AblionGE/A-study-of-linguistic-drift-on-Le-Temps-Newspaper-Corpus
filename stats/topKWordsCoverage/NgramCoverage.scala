import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object NgramCoverage {

  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("Ngram Coverage"))

    var coverage = List[(Int, Double)]()

    // The top most used percentage, in our list of 1-grams (word, count), of words.
    val percentOfTopWords = args(0).toInt
    
    for (i <- 1840 to 1998) {
      val lines = sc.textFile("hdfs:///projects/linguistic-shift/cor_ngrams/1-grams/" + i + "-*")
      val splittedLines = lines.map(_.split("\t"))
      val data = splittedLines.map(x => (x(0).toInt, x(1)))
      data.cache

      val dataSize = data.count()

      println("Data size : " + dataSize)

      // this variable keeps only the count column of the data array
      val countOnlyData = data.map(_._1)
      // Total numbers of words used in one year (cumulative : "hello hello" counts 2 words)
      val totalWordsCountInYear: Long = countOnlyData.reduce(_+_)

      // The percentage of data we want to take (data is sorted by the count of words, so we will get the top k% most
      // used words)
      val numberOfWordsToTake: Long = (percentOfTopWords * dataSize) / 100
      val topKWordsLocal = data.sortBy(_._1, false).take(numberOfWordsToTake.toInt)
      val topKWords = sc.parallelize(topKWordsLocal)

      // The count of the top k% words.
      val countOnlyTopKWords = topKWords.map(_._1)
      val totWordsTopK: Long = countOnlyTopKWords.reduce(_+_)

      val localCoverage = (totWordsTopK * 100) / totalWordsCountInYear.toDouble

      coverage = coverage:::List((i.toInt, localCoverage.toDouble))
    }

    val RDDedCoverage = sc.parallelize(coverage)
    RDDedCoverage.saveAsTextFile("hdfs:///projects/linguistic-shift/topKWordsCoverage/" + percentOfTopWords + "-percent")

    sc.stop()
  }
}

