package com.neu.mr.spark.preprocessor

import org.apache.spark.SparkContext
import com.neu.mr.constants.AppConstants
import org.apache.spark.rdd.RDD
import com.neu.mr.inhomeparser.Bz2Parser
import java.io.Serializable

class Preprocessor(sc: SparkContext) extends Serializable{
 
  /**
   * Return the link graph structure with adjacency list of all the outlinks for each page.
   */
  def run(input: String) : RDD[(String, List[String])] = {
    
    val inputLines = sc.textFile(input, sc.defaultParallelism);
    
    val parsedLines = parse(inputLines);
    
    return createGraph(parsedLines);
  }
  
  /**
   * Parses the Input HTML lines using a Java in-home parser
   */
  def parse(inputLines: RDD[String]) : RDD[String] = {
    
    return inputLines
              .map(line => Bz2Parser.parseLine(line))
              .filter(line => line.length >= 1)
              .flatMap(line => line.split(AppConstants.NEW_LINE));
  }
  
  /**
   * Creates an adjacency graph from all the parsed lines.
   */
  def createGraph(parsedLines: RDD[String]) : RDD[(String, List[String])] = {
    
    parsedLines
          .map(convertLineToGraph)
          .reduceByKey((a, b) => (a ++ b));
               
  }
  
  /**
   * Creates an adjacency graph for each of the parsed line
   */
  def convertLineToGraph = {
    (parsedLine: String) => 
      
      val lineParts = parsedLine.split(AppConstants.TILDE + AppConstants.TILDE);
      val pageName = lineParts(0);
      if(lineParts.size == 2){
        
    	  val outlinks = lineParts(1).split(AppConstants.TILDE).toList;
    	  (pageName, outlinks);

      }else{
        (pageName, List[String]());  
      }
  }
}