package es.upm.oeg.siminwikart

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

/**
 * Created by cbadenes on 03/08/15.
 */
object ObtainArticles {


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

    val file = new File("output")

    if (file.exists) FileUtils.cleanDirectory(file)


    val linksRel = sc.textFile("wikipedia/articles_links.csv").map(_.split(",")).filter(x=>x.size == 2).map(x=>(x(0),x(1)))

    val articlesId = sc.textFile("wikipedia/articles_ids.csv").map(_.split(",")(0))

    var matched = false

    var nodes = articlesId.takeSample(false,5,12345).toList

    while(!matched){

      println("reading links...")
      val links = linksRel.filter(x=>nodes.contains(x._1) || nodes.contains(x._2)).cache

      println("reading new nodes...")
      val newNodes = links.flatMap(x=>List(x._1,x._2)).distinct.collect.diff(nodes)

      if (!newNodes.isEmpty) nodes ::= newNodes(0)


      println("nodes: " + nodes.size + "->" + nodes)
      println("links: " + links.collect.size)
      matched = nodes.size > 10

    }

    println("reading links...")
    val graph = linksRel.filter(x=>nodes.contains(x._1) && nodes.contains(x._2)).cache
    graph.saveAsTextFile("output/graph2")



    val end = System.currentTimeMillis

    println("elapsed time: " + (end-start) + " msecs")


  }


}
