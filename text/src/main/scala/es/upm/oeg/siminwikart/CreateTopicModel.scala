package es.upm.oeg.siminwikart

import java.io.File

import es.upm.oeg.epnoi.matching.metrics.domain.entity._
import es.upm.oeg.epnoi.matching.metrics.domain.space.{ConceptsSpace, TopicsSpace}
import es.upm.oeg.epnoi.matching.metrics.feature.LuceneTokenizer
import es.upm.oeg.epnoi.matching.metrics.similarity.TopicsSimilarity
import es.upm.oeg.epnoi.matching.metrics.topics.LDASettings
import org.apache.commons.io.FileUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.Map

/**
 * Created by cbadenes on 03/08/15.
 */
object CreateTopicModel {


  def main(args: Array[String]): Unit = {

    // Spark Configuration
    val conf = new SparkConf().
      setMaster("local[4]").
      setAppName("Local Spark Example").
      set("spark.executor.memory", "8g").
      set("spark.driver.maxResultSize","4g")
    val sc = new SparkContext(conf)

    Logger.getRootLogger.setLevel(Level.WARN)

    val start = System.currentTimeMillis

    // LDA Settings
    LDASettings.setTopics(3);
    LDASettings.setAlpha(6.1);
    LDASettings.setBeta(1.1);
    LDASettings.setMaxIterations(200);


    val input: RDD[(String,String)]  = sc.textFile("wikipedia/sample_body.csv").
      map(x=>x.split("\",\"")).map(x=>(x(0)+"\"","\""+x(1)))

    val author: Author  = Author("oeg.es/edbt/author/000001","Wiki","Pedia")

    val regularResources : RDD[RegularResource] = input.map{case x=>
      val name = x._1.replace("\"","")
      RegularResource(
      uri         = s"oeg.es/edbt/article-"+name,
      url         = name,
      metadata    = Metadata(name,"2011",List(author)),
      bagOfWords  = LuceneTokenizer(x._2),
      resources   = Seq.empty)}

    // Conceptual Resources
    println("creating conceptual resources..")
    val conceptualResources = regularResources.map(ConceptualResource(_))

    // Conceptual Space
    println("creating a conceptual space..")
    val conceptualSpace = new ConceptsSpace(conceptualResources)

    println("creating the topics space ..")
    val topicSpace: TopicsSpace = new TopicsSpace(conceptualSpace)

    // Print Topic Details
    println("reading semantical resources..")
    val semanticResources       = topicSpace.topicalResources;

    val matrix: RDD[(TopicalResource, Iterable[(TopicalResource, TopicalResource, Double)])] = semanticResources.cartesian(semanticResources).map{case(sr1,sr2)=>(sr1,sr2,TopicsSimilarity(sr1.topics,sr2.topics))}.groupBy(_._1)

    val resourceSize = conceptualResources.count
    val matrixSize = matrix.count
    println(s"Resource Size: $resourceSize and Matrix Size: $matrixSize")

    val similarities = matrix.
      flatMap(x=>x._2).
      map(x=>(x._1.conceptualResource.resource.url,x._2.conceptualResource.resource.url,x._3)).cache()

    val file = new File("output")

    if (file.exists) FileUtils.cleanDirectory(file)

    // Create 'corpus_sim_lda.csv' file
    similarities.map(x=>x._1+","+x._2+","+x._3).saveAsTextFile("output/corpus_sim_lda")


    // Distribution of topics by documents
    val resourcesMap: Map[Long, ConceptualResource] = conceptualSpace.conceptualResourcesMap.collectAsMap;

    val topicDistribution : RDD[(ConceptualResource, Integer)] = topicSpace.model.ldaModel.topicDistributions.map{case (id,v) => (resourcesMap.getOrElse(id,new ConceptualResource(new RegularResource("","",new Metadata("","",Seq.empty),Seq.empty,Seq.empty))),HigherValue(v.toArray))}

    // Create 'corpus_sim_lda.csv' file
    topicDistribution.map(x=>x._1.resource.url+","+x._2).saveAsTextFile("output/corpus_topic")

    // Distribution of words by topics
    val topics: Array[(Array[Int], Array[Double])] = topicSpace.model.ldaModel.describeTopics()

    val wordsOfTopics : Array[Array[String]] = topics.map(x=>x._1.map(y=>conceptualSpace.vocabulary.wordsByKeyMap.getOrElse(y,"-error-")))

    // Create 'topics_words.csv' file
    sc.parallelize(wordsOfTopics).map(x=>x.toList.slice(0,20) mkString(",") ).saveAsTextFile("output/topics_words")

    val end = System.currentTimeMillis

    println("elapsed time: " + (end-start) + " msecs")


  }


}
