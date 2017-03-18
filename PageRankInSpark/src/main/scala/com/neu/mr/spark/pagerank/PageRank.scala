package com.neu.mr.spark.pagerank

import org.apache.spark.rdd.RDD
import scala.collection.mutable.ListBuffer
import org.apache.spark.SparkContext
import javax.security.auth.login.AppConfigurationEntry
import com.neu.mr.constants.AppConstants
import java.io.Serializable

class PageRank(sc: SparkContext, totalPages: Long) extends Serializable {
 
  def run(pageRankGraph: RDD[(String, (Double, List[String]))], danglingFactor: Double) : RDD[(String, (Double, List[String]))] = {
    println("Calculating page rank");
    val outlinksContribution = pageRankGraph.flatMap{tuple =>
      
//    Adding the page itself with zero as contribution, this is to maintain the graph structure.
      val pageRankNode = (tuple._1, (0.0, tuple._2._2));
      
      val outlinkSize = tuple._2._2.size;
      val pageRank = tuple._2._1;
      
      println("PageName: " + tuple._1);
      println("PageRank before: " + pageRank);
      println("Outlinks: " + outlinkSize);
      
//    Distributing the page rank values among the outlinks
      val pageRankForOutlink = (outlink: String) => (outlink, ((pageRank / outlinkSize.doubleValue()), List[String]()));
      tuple._2._2.map(pageRankForOutlink).union(List(pageRankNode));
    }
    
//  Calculating the page rank contribution as part of the page rank computation formula
    val pageRankContribution = (AppConstants.ALPHA_VALUE / totalPages.doubleValue()) + (AppConstants.INVERSE_ALPHA_VALUE * danglingFactor);
    
//  Combining the page rank contribution from all the incoming nodes and combining across partitions.
//    val createPageRankCombiner = (valueTuple: (Double, List[String])) => (((valueTuple._1 * AppConstants.INVERSE_ALPHA_VALUE) + pageRankContribution), valueTuple._2);
//    val pageRankCombiner = (accTuple: (Double, List[String]), valueTuple: (Double, List[String])) => 
//                                  (((valueTuple._1 * AppConstants.INVERSE_ALPHA_VALUE) + accTuple._1), accTuple._2.union(valueTuple._2));
//    val pageRankMerger = (accTuple1: (Double, List[String]), accTuple2: (Double, List[String])) => 
//                                  ((accTuple2._1 + accTuple1._1), accTuple1._2.union(accTuple2._2));
//    outlinksContribution.combineByKey(createPageRankCombiner, pageRankCombiner, pageRankMerger);
    
    outlinksContribution
          .reduceByKey((tuple1, tuple2) => (tuple1._1 + tuple2._1, tuple1._2.union(tuple2._2)))
          .map(tuple => (tuple._1, (((AppConstants.ALPHA_VALUE / totalPages.doubleValue()) + (AppConstants.INVERSE_ALPHA_VALUE * (danglingFactor + tuple._2._1))), tuple._2._2)));
  }
}