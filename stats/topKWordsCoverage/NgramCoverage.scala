import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object NgramCoverage {

  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("Ngram Coverage"))

    var coverage = List[(Int, Int, Double)]()
    var percentCovered: Double = 0
    var numberofWordsToStart: Int = 1
    var numberOfWordsToEnd: Int = 1

    val tempLines = sc.textFile("hdfs:///projects/linguistic-shift/corrected_ngrams/1-grams/" + 1900 + "-*")
    val tempSplittedLines = tempLines.map(_.split("\t"))
    val data = tempSplittedLines.map(x => (x(0), x(1).toInt))
    data.cache
    while (percentCovered < 80) {
      val dataSize = data.count()
      println("Data size : " + dataSize)

      // this variable keeps only the count column of the data array
      val countOnlyData = data.map(_._2)
      // Total numbers of words used in one year (cumulative : "hello hello" counts 2 words)
      val totalWordsCountInYear: Long = countOnlyData.reduce(_+_)

      // The percentage of data we want to take (data is sorted by the count of words, so we will get the top k% most
      // used words)
      val topKWordsLocal = data.sortBy(_._2, false).take(numberofWordsToStart.toInt)
      val topKWords = sc.parallelize(topKWordsLocal)

      // The count of the top k% words.
      val countOnlyTopKWords = topKWords.map(_._2)
      val totWordsTopK: Long = countOnlyTopKWords.reduce(_+_)

      percentCovered = (totWordsTopK * 100) / totalWordsCountInYear.toDouble
      numberofWordsToStart = numberofWordsToStart + 1000;
    }

    numberOfWordsToEnd = numberofWordsToStart;

    while (percentCovered < 95) {
      val dataSize = data.count()

      println("Data size : " + dataSize)

      // this variable keeps only the count column of the data array
      val countOnlyData = data.map(_._2)
      // Total numbers of words used in one year (cumulative : "hello hello" counts 2 words)
      val totalWordsCountInYear: Long = countOnlyData.reduce(_+_)

      // The percentage of data we want to take (data is sorted by the count of words, so we will get the top k% most
      // used words)
      val topKWordsLocal = data.sortBy(_._2, false).take(numberOfWordsToEnd.toInt)
      val topKWords = sc.parallelize(topKWordsLocal)

      // The count of the top k% words.
      val countOnlyTopKWords = topKWords.map(_._2)
      val totWordsTopK: Long = countOnlyTopKWords.reduce(_+_)

      percentCovered = (totWordsTopK * 100) / totalWordsCountInYear.toDouble
      numberOfWordsToEnd = numberOfWordsToEnd + 1000;
    }

    val years = List(1840, 1900, 1950, 1995)
    var j: Int = numberofWordsToStart
    while (j < numberOfWordsToEnd) {
      var tempCoverage = List[(Int, Int, Double)]()
      for (i <- years) {
        val lines = sc.textFile("hdfs:///projects/linguistic-shift/corrected_ngrams/1-grams/" + i + "-*")
        val splittedLines = lines.map(_.split("\t"))
        val data = splittedLines.map(x => (x(0), x(1).toInt))
        data.cache

        val dataSize = data.count()

        println("Data size : " + dataSize)

        // this variable keeps only the count column of the data array
        val countOnlyData = data.map(_._2)
        // Total numbers of words used in one year (cumulative : "hello hello" counts 2 words)
        val totalWordsCountInYear: Long = countOnlyData.reduce(_+_)

        // The percentage of data we want to take (data is sorted by the count of words, so we will get the top k% most
        // used words)
        val topKWordsLocal = data.sortBy(_._2, false).take(j)
        val topKWords = sc.parallelize(topKWordsLocal)

        // The count of the top k% words.
        val countOnlyTopKWords = topKWords.map(_._2)
        val totWordsTopK: Long = countOnlyTopKWords.reduce(_+_)

        val localCoverage = (totWordsTopK * 100) / totalWordsCountInYear.toDouble

        tempCoverage = tempCoverage:::List((i.toInt, j, localCoverage.toDouble))
      }
      coverage = coverage:::tempCoverage
      j = j + 1000;
    }

    val RDDedCoverage = sc.parallelize(coverage)
    RDDedCoverage.saveAsTextFile("hdfs:///projects/linguistic-shift/stats/coverage_test/coverage")

    sc.stop()
  }
}

