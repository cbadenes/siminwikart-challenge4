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
object HigherValue {


  def apply(args: Array[Double]): Integer = {
    args.indexOf(args.max)
  }

  def main(args: Array[String]): Unit = {

    val input = List(2.0,1.0,3.0)

    println(HigherValue(input.toArray))

    println(input.slice(0,2))

    val file = new File("output")

    println(file.exists)

    if (file.exists) FileUtils.cleanDirectory(file)

  }


}
