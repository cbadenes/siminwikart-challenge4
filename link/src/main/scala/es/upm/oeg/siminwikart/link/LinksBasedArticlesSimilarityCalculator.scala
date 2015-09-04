package es.upm.oeg.siminwikart.link

import es.upm.oeg.epnoi.matching.metrics.domain.entity.{ConceptualResource, RegularResource}
import es.upm.oeg.epnoi.matching.metrics.domain.space.{ConceptsSpace, TopicsSpace}
import es.upm.oeg.epnoi.matching.metrics.topics.LDASettings
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.RDD

class LinksBasedArticlesSimilarityCalculator {
  
  def getSetOfLinks(articlesLinks:RDD[Array[String]], articleID : String) 
  : Set[String] = {
    println("getting incoming/outgoing links of the passed article ...")
    println("generating pairLinksInList ...")
    val pairLinksInList = articlesLinks.filter(
        x => x(0).equals(articleID) || x(1).equals(articleID));
    println("pairLinksInList = " + pairLinksInList);
    
    println("generating linksInSet ...")
    val linksInSet = pairLinksInList.map(x => {
      if(x(0).equals(articleID)) { x(1)}
      else { x(0) }
    } ).collect.toSet
    linksInSet;
  }
  
  def calculateSimilarities(linkSet1:Set[String], linkSet2:Set[String]) : Int = {
    println("linkSet1.size = " + linkSet1.size);
    println("linkSet2.size = " + linkSet2.size);
    val intersectionSet = linkSet1.intersect(linkSet2);
    println("intersectionSet = " + intersectionSet);
    println("intersectionSet.size = " + intersectionSet.size);
    
    val similarityValue = intersectionSet.size / ((linkSet1.size + linkSet2.size) / 2);
    similarityValue;
  }
  
  def calculateSimilarities(articlesLinks:RDD[Array[String]]
  , article1ID:String, article2ID:String) : Int = {
    val linkSet1 = this.getSetOfLinks(articlesLinks, article1ID);
    val linkSet2 = this.getSetOfLinks(articlesLinks, article2ID);
    val similarityValue = this.calculateSimilarities(linkSet1, linkSet2);
    similarityValue;
  }
}
/**
 * Created by fpriyatna on Sept 3rd, 2015
 */
object LinksBasedArticlesSimilarityCalculator {

//  def apply (corpus: RDD[RegularResource], topics: Integer, alpha: Double, beta: Double, maxIt: Integer): (ConceptsSpace,TopicsSpace)= {


  def main(args: Array[String]): Unit = {
    val testObject : LinksBasedArticlesSimilarityCalculator = new LinksBasedArticlesSimilarityCalculator();
    
    // Spark Configuration
    val conf = new SparkConf().
      setMaster("local[4]").
      setAppName("Local Spark Example").
      set("spark.executor.memory", "6g").
      set("spark.driver.maxResultSize","4g")
    val sc = new SparkContext(conf)

    Logger.getRootLogger.setLevel(Level.WARN)

    val start = System.currentTimeMillis

    println("getting list of articles links...")
    val articlesLinksFileURL = "/home/freddy/Downloads/articles_links.csv";
    val articlesLinks = sc.textFile(articlesLinksFileURL).
      map(_.replace("\"","").split(",")).filter(_.size>1)
      //map(x=>x.replace("\"","").split(","))
    println("list of articles links obtained.")
    
    val nobelPrizeAID = "21201";
    
    
    
    val barackObamaAID = "534366";
    val monetDBAID = "125179";
    val linksBarackObamaSet= testObject.getSetOfLinks(articlesLinks, barackObamaAID);
    println("linksBarackObamaSet = " + linksBarackObamaSet);
    println("linksBarackObamaSet.size = " + linksBarackObamaSet.size);
     
    val severoOchoaAID= "182077";
    val neo4JAID = "26231804";
    val linksSeveroOchoaSet = testObject.getSetOfLinks(articlesLinks, severoOchoaAID);
    println("linksSeveroOchoaSet = " + linksSeveroOchoaSet);
    println("linksSeveroOchoaSet.size = " + linksSeveroOchoaSet.size);
    
    //val similarityValue = testObject.calculateSimilarities(linksBarackObamaSet, linksSeveroOchoaSet);
    val similarityValue = testObject.calculateSimilarities(articlesLinks
        , monetDBAID, neo4JAID);
    println("similarityValue = " + similarityValue);
    
    
//    println("loading articles body for selected articles...")
//    val selectedArticles = sc.textFile("/Users/cbadenes/Documents/OEG/SummerSchool/EDBT2015/challenge/articles_body.csv").
//      map(row=>row.split("\",\"")).
//      filter(_.size == 2).
//      map(x=>(x(0).replace("\"",""),x(1).replace("\"",""))).
//      filter(x=>articles.contains(x._1))
//
//    selectedArticles.saveAsTextFile("nobel_prize_articles")

    val end = System.currentTimeMillis

    println("elapsed time: " + (end-start) + " msecs")
//      // LDA Settings
//    LDASettings.setTopics(topics);
//    LDASettings.setAlpha(alpha);
//    LDASettings.setBeta(beta);
//    LDASettings.setMaxIterations(maxIt);
//
//    // Conceptual Resources
//    println("creating conceptual resources..")
//    val conceptualResources = corpus.map(ConceptualResource(_))
//
//    // Conceptual Space
//    println("creating a conceptual space..")
//    val conceptualSpace = new ConceptsSpace(conceptualResources)
//
//    println("creating the topics space ..")
//    val topicSpace: TopicsSpace = new TopicsSpace(conceptualSpace)
//
//    return (conceptualSpace,topicSpace)

  }


}
