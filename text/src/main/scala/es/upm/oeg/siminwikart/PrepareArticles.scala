package es.upm.oeg.siminwikart

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.Map

/**
 * Created by cbadenes on 03/08/15.
 */
object PrepareArticles {


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

    val articlesId : List[String]       = sc.textFile("output/finalNodes/finalNodes.csv").collect.toList


    val corpus = sc.textFile("wikipedia/articles_body.csv").
      map(x=>x.split("\",\"")).filter(x=>articlesId.contains(x(0).replace("\"","")))

    // Create file 'corpus_body.csv'
    corpus.map(x=>x mkString("\",\"")).saveAsTextFile("wikipedia/corpus_body")

    val end = System.currentTimeMillis

    println("elapsed time: " + (end-start) + " msecs")


  }


}
