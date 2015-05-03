package ch.epfl.bigdata

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{ Vector, Vectors }
import org.apache.spark.mllib.clustering.LDA
import org.apache.spark.SparkConf
import scala.collection.immutable.Map

object TermIndexing {

  def main(args: Array[String]) {

	val sc = new SparkContext( new SparkConf().setAppName("Term Indexing"))
	val lines = sc.textFile(args(0)) // /projects/linguistic-shift/nGramArticle/nGramArticle/1998-r-00021

    //get only the words from the nGramArticleFormat
    val words = lines.map(_.split("\t")).map(_.map(x => x.replaceAll("[^a-zA-Z]", "")))
    //get all distinct words with length at least equal to 2 and zip them with unique index that would be column number of term
    val withoutStop = words.flatMap(_.filter(elem => elem.length > 3)).distinct.zipWithIndex.cache()
    withoutStop.saveAsTextFile("./withoutStop")
//    val articleGram = lines.map(_.split(",")).map(elem => elem.map(str => str.split("\t")).map(x => if (x.length == 3) Array(x(1), x(2)) else x)).zipWithIndex
//    val tuplesToJoin = articleGram.flatMap(x => x._1.map(y => (y(0), (y(1), x._2)))) //(word:String, (occ:String, articleId:Long))
//    val joined = withoutStop.join(tuplesToJoin).map(e => (e._2._2._2, (e._2._1.toInt, e._2._2._1.toDouble)))//(mot:String,(Column:Long, (Occ:String,artID:Long))) 
//    val tmp = joined.groupByKey //(Long, Iterable[(Int, Double)]
    val tuplesToJoin = lines.map(_.split(",")).flatMap(elem => elem.map(str => str.split("\t")).map(x => (x(1),((x(2),x(0))))))
  println(" YOLO FINISHED :28 ");
    val joined = withoutStop.join(tuplesToJoin).map(e => (e._2._2._2, (e._2._1.toInt, e._2._2._1.toDouble))).groupByKey//(mot:String,(Column:Long, (Occ:String,a
  println(" YOLO FINISHED :29 ");
    val doc_term_matrix = joined.map {
      case (artId, it) =>{
    	  val label = artId.replace("//","")
    	  val lists = it.foldRight(List[Int](),List[Double]())((acc, cur) =>(acc._1::cur._1, acc._2::cur._2 ))
    	  (artId.toLong, Vectors.sparse(it.size,lists._1.toArray, lists._2.toArray ))

      }
    }
	

    val numTopics = args(1).toInt
    val lda = new LDA().setK(numTopics).setMaxIterations(10)
    val ldaModel = lda.run(doc_term_matrix)
    //val vocabArray = withoutStop.keyBy(elem=>elem._2.toInt)
    // Print topics, showing top-weighted 10 terms for each topic.
    val topicIndices = ldaModel.describeTopics(maxTermsPerTopic = 10)
    topicIndices.foreach {
      case (terms, termWeights) =>
        println("TOPIC:")
        val tmp = terms.zip(termWeights).foreach {
          case (term, weight) =>{
              println(s"${term}\t$weight")
          }
        }
        println()
    }

  }
}