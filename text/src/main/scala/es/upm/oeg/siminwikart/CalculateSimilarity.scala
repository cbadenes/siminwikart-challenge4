package es.upm.oeg.siminwikart

import java.io.File

import es.upm.oeg.epnoi.matching.metrics.domain.entity._
import es.upm.oeg.epnoi.matching.metrics.domain.space.{ConceptsSpace, TopicsSpace}
import es.upm.oeg.epnoi.matching.metrics.feature.LuceneTokenizer
import es.upm.oeg.epnoi.matching.metrics.similarity.TopicsSimilarity
import es.upm.oeg.epnoi.matching.metrics.topics.{TopicModel, LDASettings}
import org.apache.commons.io.FileUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.Map

/**
 * Created by cbadenes on 03/08/15.
 */
object CalculateSimilarity {

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

    println("loading semantic resources..")
    val semanticResources : RDD[TopicalResource] = sc.objectFile("text/model/lda-sr")

    val semArticles = semanticResources.collect.toList

    val articles = List("507433","240390","2484564","2150841").
      map(x=>semArticles.filter(y=>y.conceptualResource.resource.url.equals(x))(0))

    // Show distribution of topics
    articles.foreach{case article=>
      println(s"Topic Distribution from '"+article.conceptualResource.resource.url+"': "
        + article.topics.distribution.toList )
    }

    // Calculate the similarity
    articles.flatMap(x=>articles.map(y=>(x,y))).
      map(x=>(x._1,x._2,TopicsSimilarity(x._1.topics,x._2.topics))).
      foreach(x=>println(s"The similarity between '"+x._1.conceptualResource.resource.url
      +"' and '"+x._2.conceptualResource.resource.url+"' is " + x._3))

    val end = System.currentTimeMillis

    println("elapsed time: " + (end-start) + " msecs")

  }


}
