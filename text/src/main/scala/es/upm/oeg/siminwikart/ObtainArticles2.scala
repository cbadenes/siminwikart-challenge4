package es.upm.oeg.siminwikart

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by cbadenes on 03/08/15.
 */
object ObtainArticles2 {


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
    val ref   = "\"21201\""

    println("reading links...")
    val articlesLinks = sc.textFile("wikipedia/articles_links.csv").
        map(_.split(",")).filter(x=>x.size == 2 && (x(0).equals(ref) || x(1).equals(ref)))

    val incoming = articlesLinks.filter(x=>x(1).equals(ref)).map(x=>x(0)).collect.toList

    val nodesAndCateg = sc.textFile("wikipedia/article_category.csv").
          map(_.split(",")).filter(x=>incoming.contains(x(0))).map(x=>(x(1),x(0))).groupByKey.flatMap(x=>x._2.slice(0,9).map(y=>(y,x._1)))

    val selectedNodes = nodesAndCateg.map(x=>x._1).distinct.collect.toList.slice(0,100)

    println("selectedNodes: " + selectedNodes.size + " -> " + selectedNodes)

    val incomingLinks = sc.textFile("wikipedia/articles_links.csv").
      map(_.split(",")).filter(x=>x.size == 2 && (selectedNodes.contains(x(0)) && selectedNodes.contains(x(1))))

    incomingLinks.map(x=>x.mkString(",")).saveAsTextFile("output/graph")

    println("Incoming Links: " + incomingLinks.collect.size)

    val nodes = incomingLinks.flatMap(x=>List(x(0),x(1))).distinct

    println("Number of nodes: " + nodes.collect.size)

    nodes.map(x=>x.mkString(",")).saveAsTextFile("output/nodes")



//    val finalNodes = sc.textFile("output/graph/graph.csv").
//      flatMap(_.split(",")).map(x=>(x,1)).reduceByKey((x,y)=>x+y).filter(x=>x._2>2).map(x=>x._1).collect.toList
//
//
//    println("Nodes: " + finalNodes.size + "->" + finalNodes )
//
//    val finalLinks = sc.textFile("output/graph/graph.csv").
//      map(_.split(",")).filter(x=>x.size == 2 && (finalNodes.contains(x(0)) || finalNodes.contains(x(1))))
//
//
//    finalLinks.map(x=>x mkString (",")).saveAsTextFile("output/finalGraph")


//    val finalNodes = sc.textFile("output/finalGraph/finalGraph.csv").
//      flatMap(_.split(",")).distinct
//
//     finalNodes.saveAsTextFile("output/finalNodes")


//    val finalNodes = sc.textFile("output/finalGraph/finalGraph.csv").
//      map(_.split(",")).map(x=>(x(0),x(1))).groupByKey.filter(x=>x._2.size>1).collect.toList
//
//    println("Final Nodes: " + finalNodes.size + "->" + finalNodes)




    val end = System.currentTimeMillis

    println("elapsed time: " + (end-start) + " msecs")


  }


}
