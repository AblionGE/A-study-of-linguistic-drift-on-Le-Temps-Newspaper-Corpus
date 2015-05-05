
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{ Vector, Vectors }
import org.apache.spark.mllib.clustering.LDA
import org.apache.spark.SparkConf
import scala.collection.immutable.Map
import org.apache.spark.mllib.clustering.LDAModel

object TermIndexing {

  def main(args: Array[String]) {
    if(args.length!=3) throw new Exception("Arguments should be path of corpus, number of Topics, treshold for stop words")

    val sc = new SparkContext(new SparkConf().setAppName("Topic Clustering"))
    val lines = sc.textFile(args(0)) //"hdfs:///projects/linguistic-shift/corrected_nGramArticle/nGramArticle/*"

    //get only the words from the nGramArticleFormat
    val words = lines.map(_.split("\t")).map(_.map(x => x.replaceAll("[^a-zA-Z]", "")))
    //get all distinct words with length at least equal to 2 and zip them with unique index that would be column number of term
    val withoutStop = words.flatMap(_.filter(elem => elem.length > 3)).distinct.zipWithIndex.cache()
   // withoutStop.saveAsTextFile("./withoutStop")
    
    val tuplesToJoin = lines.map(_.split(",")).flatMap(_.map(_.split("\t"))).map(x => if(x.length==3) (x(1), ((x(2), x(0)))) else (x(2),(x(3),x(1)))).filter(elem=>(elem._2._1).toInt<args(2).toInt)
    val joined = withoutStop.join(tuplesToJoin).map(e => (e._2._2._2, (e._2._1.toInt, e._2._2._1.toDouble))).groupByKey //(artID, <(numColm, occ)>)
   
    val docTermMatrix = joined.map {
      case (artId, it) => {
        val label = artId.split("//")(1)
        val lists = it.foldRight(List[Int](), List[Double]())((acc, cur) => (acc._1 :: cur._1, acc._2 :: cur._2))
        (label.toLong, Vectors.sparse(it.size, lists._1.toArray, lists._2.toArray))

      }
    }

    val numTopics = args(1).toInt
      //for(beta<-Range(1,17,4)){
//    	println("Alpha= "+beta);
	    val lda = new LDA().setK(numTopics).setMaxIterations(10).setBeta(3+0.1)
	    val ldaModel = lda.run(docTermMatrix)
	    val topicIndices = ldaModel.describeTopics(maxTermsPerTopic = 10)
	    for (topic <- Range(0,topicIndices.length)){
	      println("TOPIC");
	      for(term<- Range(0,topicIndices(0)._1.length)){
	    	  println("("+topicIndices(topic)._1(term)+" , "+topicIndices(topic)._2(term)+")")
	      }
	    }
      //}
    

//   
//    topicIndices.foreach {
//      
//      case (terms, termWeights) =>
//        println("TOPIC:")
//        terms.zip(termWeights).foreach {
//          case (term, weight) => {
//        	val res = withoutStop.filter(elem=> elem._2.toInt == term)//.saveAsTextFile(path)
//            println(s"${term}\t$weight")
//          }
//        }
//        println()
//    }

  }
}