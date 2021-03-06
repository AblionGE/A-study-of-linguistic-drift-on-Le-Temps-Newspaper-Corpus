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
/**
 * @author Jeremy Weber, Farah Bouassida
 */
object TermIndexing {

  def main(args: Array[String]) {

    if (args.size != 2) {
      println("Use with 2 args : path of the ngrams by article, \"hdfs:///projects/linguistic-shift/corrected_nGramArticle/nGramArticle/*\" ,and the number of topics (15) otherwise modify the parser")
      exit(1)
    }

    /**
     * lines: The path of the ngram articles, we use hdfs:///projects/linguistic-shift/corrected_nGramArticle/nGramArticle/*"
     * */
     * numTopics: Number of topics wanted
     * FreqThreshold: Maximum tolerated frequency for a term by article before being considered as a stopword
     */
    val sc = new SparkContext(new SparkConf().setAppName("Topic Clustering"))
    val lines = sc.textFile(args(0))
    val numTopics = args(1).toInt
    val FreqThreshold = 4

    /**
     * Firt part is to get all distinct words with length at least equal to 3
     * and zip them with unique index that would represent the column number of the term
     */
    val words = lines.map(_.split("\t")).map(_.map(x => x.replaceAll("[^a-zA-Z]", "")))
    val dictionnary = words.flatMap(_.filter(elem => elem.length > 3)).distinct.zipWithIndex.cache()

    //A list of some observed stop words
    val stopwords = Array("pour", "dans", "cette", "suisse", "tout", "avec", "mais", "tous", "sont", "plus", "deux", "elles", "etre", "elle", "leur", "comme", "meme")
    /**
     * Process to obtain a post processed list of 1-grams by article ID.
     * Joined is in format : RDD[(artID:String, Iterable[(numColm:Int, occ:Double)])]
     * gramOcc: gather an iterable of 1-grams and its occurences tuples by article ID to write them back depending on the topic at the end.
     */
    val tuplesToJoin = lines.map(_.split(",")).flatMap(_.map(_.split("\t"))).map(x => if (x.length == 3) (x(1), ((x(2), x(0)))) else (x(2), (x(3), x(1)))).filter(elem => (elem._2._1).toInt < FreqThreshold)
    val gram = tuplesToJoin.filter(x => !stopwords.contains(x._1))
    val joined = dictionnary.join(gram).map(e => (e._2._2._2, (e._2._1.toInt, e._2._2._1.toDouble))).groupByKey
    val gramOcc = gram.map(e => (e._2._2, (e._1, e._2._1))).groupByKey

    /**
     * Term document matrix of the format:
     * RDD[(artId:String, Iterable[(term_comlumn:Int, term_occurence:Double)])]
     */
    val docTermMatrix = joined.map {
      case (artId, it) => {
        val label = artId.split("//")(1)
        val lists = it.foldRight(List[Int](), List[Double]())((acc, cur) => (acc._1 :: cur._1, acc._2 :: cur._2))
        (label.toLong, Vectors.sparse(it.size, lists._1.toArray, lists._2.toArray))

      }
    }

    /**
     * LDA processing part:
     *  @param Beta Distribution concentration of topics over terms
     *  @param Alpha Distribution concentration of articles over topics
     *  The constants were defined after lot of hyperparmeter optimization steps
     */

    val Beta = 17
    val Alpha = 1.1

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

    /**
     * Creation of files by year by topics
     * save the tuples ID year//unique_artId_Long, Vector of article probality distribution over topics in a file
     * to get back the vector values from the file
     */
    val idMapping = tuplesToJoin.map(x => (x._2._2, (x._1, x._2._1))).map {
      case (id, tuple) =>
        ((id.split("//")(1)).toLong, id)
    }
    val topicDist = ldaModel.topicDistributions //(artId, Vector of topic distribution)
    val toFile = topicDist.join(idMapping).map(elem => (elem._2._2, elem._2._1)).distinct
    toFile.saveAsTextFile("./Result")

    /**
     * 2nd part: Read the previous result and do an assignment of each article to one topic
     * when taking back an array of the article topic distribution and assigning the article to the
     * topic with maximum probability (hard clustering as a newspaper article is more likely talking
     * about one topic.
     *
     */
    val result = sc.textFile("./Result/*")

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

    val topicAssignment = probabilities.map { e =>
      (e._1, max(e._2))
    }

    val toWrite = topicAssignment.join(gramOcc).map {
      case (artId, tup) => {
        val year = artId.split("//")(0)
        (tup._1, (year, tup._2))
      }

    }.groupByKey

    //    Code to write the artices in the corresponding year in the matching topic
    //        class RDDMultipleTextOutputFormat extends MultipleTextOutputFormat[Any, Any] {
    //          override def generateActualKey(key: Any, value: Any): Any =
    //            NullWritable.get()
    //    
    //          override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String =
    //            key.asInstanceOf[String]
    //    
    //        }
    //    
    //        for (i <- Range(0, 15)) {
    //    
    //          val topic = toWrite.filter(art => art._1 == i).flatMap(_._2).groupByKey.map(elem=>(elem._1,elem._2.flatten))
    //          //map(e => (e._1, e._2.foldLeft(" ")((acc, curr) => acc + "\n" + curr._1 + "\t" + curr._2))) //RDD[(String, Iterable[(Int, Double)])]
    //          val path = "Topic/topic_" + i + "_85"
    //          topic.saveAsHadoopFile(path, classOf[String], classOf[String],
    //            classOf[RDDMultipleTextOutputFormat])
    //        }

    sc.stop

  }
}

/**
 * Parsing object for 15 topics clusters
 * To add more topics you have to adapt the parser to have 5 more probablities
 */
object LineP extends RegexParsers with java.io.Serializable {
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