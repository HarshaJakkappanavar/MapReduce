package com.neu.mr.spark.pagerank

import org.apache.spark.rdd.RDD
import scala.collection.mutable.ListBuffer
import org.apache.spark.SparkContext
import javax.security.auth.login.AppConfigurationEntry
import com.neu.mr.constants.AppConstants

class PageRank(sc: SparkContext, totalPages: Long) extends Serializable {
 
  def run(pageRankGraph: RDD[(String, (Double, List[String]))], danglingFactor: Double) : RDD[(String, (Double, List[String]))] = {
    
    val outlinksContribution = pageRankGraph.flatMap{tuple =>
      
//    Adding the page itself with zero as contribution, this is to maintain the graph structure.
      val pageRankNode = (tuple._1, (0.0, tuple._2._2));
      
      val outlinkSize = tuple._2._2.size;
      val pageRank = tuple._2._1;
      
//    Distributing the page rank values among the outlinks
      val pageRankForOutlink = (outlink: String) => (outlink, ((pageRank / outlinkSize.doubleValue()), List[String]()));
      tuple._2._2.map(pageRankForOutlink).union(Seq(pageRankNode));
    }
    
//  Calculating the page rank contribution as part of the page rank computation formula
    val pageRankContribution = (AppConstants.ALPHA_VALUE / totalPages) + 
                               (AppConstants.INVERSE_ALPHA_VALUE * danglingFactor);
    
//  Combining the page rank contribution from all the incoming nodes and combining across partitions.
    val createPageRankCombiner = (tuple: (Double, List[String])) => (((tuple._1 * AppConstants.INVERSE_ALPHA_VALUE) + pageRankContribution), tuple._2);
    val pageRankCombiner = (accTuple: (Double, List[String]), valueTuple: (Double, List[String])) => 
                                  (((valueTuple._1 * AppConstants.INVERSE_ALPHA_VALUE) + accTuple._1), accTuple._2.union(valueTuple._2));
    val pageRankMerger = (accTuple: (Double, List[String]), valueTuple: (Double, List[String])) => 
                                  ((valueTuple._1 + accTuple._1), accTuple._2.union(valueTuple._2));
    outlinksContribution.combineByKey(createPageRankCombiner, pageRankCombiner, pageRankMerger);
  }
}