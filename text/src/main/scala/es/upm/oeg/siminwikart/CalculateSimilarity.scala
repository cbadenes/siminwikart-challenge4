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

    val BARACK_OBAMA_ID = "2031880"
    val SEVERO_OCHOA_ID = "42068564"

    println("loading semantic resources..")
    val semanticResources : RDD[TopicalResource] = sc.objectFile("text/model/lda-sr")

    println(semanticResources)

    val boSemanticResource = semanticResources.filter(x=>x.conceptualResource.resource.url.equals(BARACK_OBAMA_ID)).first
    val soSemanticResource = semanticResources.filter(x=>x.conceptualResource.resource.url.equals(SEVERO_OCHOA_ID)).first

    // Show distribution of topics
    println(s"Topic Distribution from '$BARACK_OBAMA_ID': " + boSemanticResource.topics.distribution.toList )
    println(s"Topic Distribution from '$SEVERO_OCHOA_ID': " + soSemanticResource.topics.distribution.toList )

    // Calculate the similarity
    val similarity = TopicsSimilarity(boSemanticResource.topics,soSemanticResource.topics)
    println(s"The similarity between '$BARACK_OBAMA_ID' and '$SEVERO_OCHOA_ID' is $similarity")

    val end = System.currentTimeMillis

    println("elapsed time: " + (end-start) + " msecs")


  }


}
