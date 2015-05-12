
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.clustering.LDA
import org.apache.spark.SparkConf
import org.apache.spark.mllib.clustering.LDAModel
import scala.util.parsing.combinator._
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.hadoop.io.NullWritable

object TermIndexing {

  def main(args: Array[String]) {

    val sc = new SparkContext(new SparkConf().setAppName("Topic Clustering"))
    val lines = sc.textFile(args(0)) //"hdfs:///projects/linguistic-shift/corrected_nGramArticle/nGramArticle/*" (For tests use 199*)
    val numTopics = args(1).toInt // Number of topics wanted 
    val FreqThreshold = 4 // Maximum tolerated frequency for a term by article before being considered as a stopword

    //get only the words from the nGramArticleFormat
    val words = lines.map(_.split("\t")).map(_.map(x => x.replaceAll("[^a-zA-Z]", "")))
    //get all distinct words with length at least equal to 3 and zip them with unique index that would represent the column number of the term
    val dictionnary = words.flatMap(_.filter(elem => elem.length > 3)).distinct.zipWithIndex.cache()
    // withoutStop.saveAsTextFile("./dictionnary")

    //A list of some observed stop words
    val stopwords = Array("pour", "dans", "cette", "suisse", "tout", "avec", "mais", "tous", "sont", "plus", "deux", "elles", "etre", "elle", "leur", "comme", "meme")
    //tuplesToJoin is in format (1-gram, (occ:String, artId:String)
    val tuplesToJoin = lines.map(_.split(",")).flatMap(_.map(_.split("\t"))).map(x => if (x.length == 3) (x(1), ((x(2), x(0)))) else (x(2), (x(3), x(1)))).filter(elem => (elem._2._1).toInt < FreqThreshold)
    val gram = tuplesToJoin.filter(x => !stopwords.contains(x._1))
    val gramOcc = gram.map(e => (e._2._2, (e._1, e._2._1))).groupByKey
    val joined = dictionnary.join(gram).map(e => (e._2._2._2, (e._2._1.toInt, e._2._2._1.toDouble))).groupByKey //RDD[(artID:String, Iterable[(numColm:Int, occ:Double)])]

    //RDD[(artId:String, Iterable[(term_comlumn:Int, term_occurence:Double)])]
    val docTermMatrix = joined.map {
      case (artId, it) => {
        val label = artId.split("//")(1)
        val lists = it.foldRight(List[Int](), List[Double]())((acc, cur) => (acc._1 :: cur._1, acc._2 :: cur._2))
        (label.toLong, Vectors.sparse(it.size, lists._1.toArray, lists._2.toArray))

      }
    }

    //LDA part : 

    val Beta = 17 // Distribution concentration of topics over terms
    val Alpha = 1.1 //Distribution concentration of articles over topics 

    val lda = new LDA().setK(numTopics).setTopicConcentration(Beta + 0.1).setMaxIterations(50).setDocConcentration(Alpha.toDouble + 0.1)
    val ldaModel = lda.run(docTermMatrix)
    //Code to describe topics with ten words in format (column number, weight)
    //    val topicIndices = ldaModel.describeTopics(maxTermsPerTopic = 10)
    //    for (topic <- Range(0, topicIndices.length)) {
    //      println("TOPIC");
    //      for (term <- Range(0, topicIndices(0)._1.length)) {
    //        println("(" + topicIndices(topic)._1(term) + " , " + topicIndices(topic)._2(term) + ")")
    //      }
    //    }

    //Creation of files by year by topics 
    val idMapping = tuplesToJoin.map(x => (x._2._2, (x._1, x._2._1))).map {
      case (id, tuple) => //art, (w,occ)
        ((id.split("//")(1)).toLong, id) //(2nd part of artId, complete artId)
    }
    val topicDist = ldaModel.topicDistributions //(artId, Vector of topic distribution)
    val toFile = topicDist.join(idMapping).map(elem => (elem._2._2, elem._2._1)).distinct //Mapping with the whole article ID
    toFile.saveAsTextFile("./Result/MapArticle")
	gramOcc.saveAsTextFile("./Results/gramOcc")
  
  
  
    //2nd part 
    val result = sc.textFile("./Result/MapArticle/*")

    def parseLine(l: String): (String, Array[Double]) =
      LineP.parse(LineP.resline, l).getOrElse(("None", Array()))

    val probabilities = result.map(l => parseLine(l))
    def max(array: Array[Double]): Int = {
      var maxIdx: Int = 0;
     var maxValue: Double = 0.0;
      var idx = 0
      for (i <- 0 until array.length) {
        if (array.apply(i) > maxValue) {
          maxValue = array.apply(i)
          maxIdx = i
        }
      }
      return maxIdx
   }

    val tmp = probabilities.map { e =>
      val res = (e._1, max(e._2))
      println("MAX =" + res._2)
      res
    }
	tmp.saveAsTextFile("./Results/ParsedResult")
//
//    val toWrite = tmp.join(gramOcc).map {
//      case (artId, tup) => {
//        val year = artId.split("//")(0)
//        (tup._1, (year, tup._2))
//      }
//
//    }.groupByKey
//
//    class RDDMultipleTextOutputFormat extends MultipleTextOutputFormat[Any, Any] {
//      override def generateActualKey(key: Any, value: Any): Any =
//        NullWritable.get()
//
//      override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String =
//        key.asInstanceOf[String]
//
//    }
//    for (i <- Range(0, 14)) {
//
//      val topic = toWrite.filter(art => art._1 == i).flatMap(_._2).map(e => (e._1, e._2.foldLeft(" ")((acc, curr) => acc + "\n" + curr._1 + "\t" + curr._2))) //RDD[(String, Iterable[(Int, Double)])]
//      val path = "Topics/topic_"+i
//      topic.saveAsHadoopFile(path, classOf[String], classOf[String],
//        classOf[RDDMultipleTextOutputFormat])
//    }

    sc.stop

  }
}

object LineP extends RegexParsers with java.io.Serializable {
  //(1992//8950645809754189824,[0.8440318794560266,0.0017369473507710196,0.004210963107485144,0.14952536120581233,4.948488799048371E-4])
  def resline: Parser[(String, Array[Double])] = (
    "(" ~ artId ~ ",["
    ~ prob ~ "," ~ prob ~ "," ~ prob ~ "," ~ prob ~ "," ~ prob ~ "," ~ prob ~ "," ~ prob ~ "," ~ prob ~ "," ~ prob ~ "," ~ prob ~ "," ~ prob ~ "," ~ prob ~ "," ~ prob ~ "," ~ prob ~ "," ~ prob ~ "])"
    ^^ {
      case _ ~ id ~ _ ~ t1 ~ _ ~ t2 ~ _ ~ t3 ~ _ ~ t4 ~ _ ~ t5 ~ _ ~ t6 ~ _ ~ t7 ~ _ ~ t8 ~ _ ~ t9 ~ _ ~ t10 ~ _ ~ t11 ~ _ ~ t12 ~ _ ~ t13 ~ _ ~ t14 ~ _ ~ t15 ~ _ =>
        (id.toString, Array(t1.toDouble, t2.toDouble, t3.toDouble, t4.toDouble, t5.toDouble, t6.toDouble, t7.toDouble, t8.toDouble, t9.toDouble, t10.toDouble, t11.toDouble, t12.toDouble, t13.toDouble, t14.toDouble, t15.toDouble))
    })
  val prob: Parser[String] = "[0-9].[0-9E-]+".r
  val artId: Parser[String] = "[0-9/]+".r
}

