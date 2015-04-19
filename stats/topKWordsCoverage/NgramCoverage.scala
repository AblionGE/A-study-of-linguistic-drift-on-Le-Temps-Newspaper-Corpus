import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object NgramCoverage {

  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("Ngram Coverage"))

    var coverage = List[(Int, Double)]()

    val percentOfTopWords = args(0).toInt
    
    for (i <- 1840 to 1998) {
      val lines = sc.textFile("hdfs:///projects/linguistic-shift/cor_ngrams/1-grams/" + i + "-*")
      val splittedLines = lines.map(_.split("\t"))
      val data = splittedLines.map(x => (x(0).toInt, x(1)))
      data.cache

      val dataSize = data.count()

      println("Data size : " + dataSize)

      val countOnlyData = data.map(_._1)
      val totalWordsCountInYear: Long = countOnlyData.reduce(_+_)

      val numberOfWordsToTake: Long = (percentOfTopWords * dataSize) / 100
      val topKWordsLocal = data.sortBy(_._1, false).take(numberOfWordsToTake.toInt)
      val topKWords = sc.parallelize(topKWordsLocal)

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

