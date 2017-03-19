package com.neu.mr.spark.program

import org.apache.spark.HashPartitioner
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import com.neu.mr.spark.pagerank.PageRank
import com.neu.mr.spark.preprocessor.Preprocessor
import java.io.Serializable

object PageRankInSpark extends Serializable{
   
  def main(args : Array[String]): Unit = {
    
    val config = new SparkConf().setAppName("PageRankInSpark").setMaster("yarn");
    val sc = new SparkContext(config);
    
    /**
     * Pre-processing the input file, parsing the line with an in-home parser
     */
    val preprocessJob = new Preprocessor(sc);
    
    val linksGraph = preprocessJob.run(args(0));
    
    val totalPages = linksGraph.count();
    
    val init_PR = 1.0 / totalPages.doubleValue();
    
    var pageRankGraph = createPageRankGraph(linksGraph, init_PR).partitionBy(new HashPartitioner(Runtime.getRuntime().availableProcessors())).persist;
    
    var danglingNodeGraph = getDanglingNodeGraph(pageRankGraph);
    var danglingFactor = sc.doubleAccumulator;
    danglingFactor.add(computeDanglingFactor(danglingNodeGraph, totalPages));
    
    /**
     * Calculating and refining the page rank
     */
    val pageRankJob = new PageRank(sc, totalPages);
    for(iteration <- 1 to 10){
    	pageRankGraph = pageRankJob.run(pageRankGraph, danglingFactor.sum);
    	
    	danglingNodeGraph = getDanglingNodeGraph(pageRankGraph);
      danglingFactor.reset();
      danglingFactor.add(computeDanglingFactor(danglingNodeGraph, totalPages));
    }
    
    /**
     * Printing the top-100 pages to output file.
     */
    /**
     * Using rdd.top()
     */
//    sc.parallelize(pageRankGraph.map(x => (x._2._1, x._1)).top(100), 1).saveAsTextFile(args(1));
    
    /** 
     *  Using rdd.take()
     */
    val top100FromPartition = pageRankGraph.map(x => (x._1, x._2._1)).takeOrdered(100)(Ordering[Double].reverse.on{(tuple: (String, Double)) => (tuple._2)});
    sc.parallelize(top100FromPartition).repartition(1).sortBy(-_._2).saveAsTextFile(args(1));
    sc.stop();
  }
  
  /**
   * Creates a dangling node graph from the link graph.
   */
  def getDanglingNodeGraph(pageRankGraph: RDD[(String, (Double, List[String]))]) : RDD[(String, (Double, List[String]))] = {
    
    return pageRankGraph
                 .filter(tuple => tuple._2._2.isEmpty);
  }
  
  /**
   * Creates a node graph with page rank from the link graph.
   */
  def createPageRankGraph(linksGraph: RDD[(String, List[String])], init_PR: Double) : RDD[(String, (Double, List[String]))] = {
    
    return linksGraph.map(tuple => (tuple._1, (init_PR, tuple._2)));
  }
  
  /**
   * Calculates the dangling contribution factor from all the dangling nodes.  
   */
  def computeDanglingFactor(danglingNodeGraph: RDD[(String, (Double, List[String]))], totalPages: Long) : Double = {
    
    val danglingContribution =  danglingNodeGraph
                                    .aggregate(0.0 : Double)(
                                              (danglingAccumulator : Double, tuple : (String, (Double, List[String]))) => (danglingAccumulator + tuple._2._1), 
                                                  (danglingAccumulator1 : Double, danglingAccumulator2 : Double) => (danglingAccumulator1 + danglingAccumulator2));
    return danglingContribution / totalPages.doubleValue();
  }
  
}