/**
 * 
 */
package com.neu.mr.constants;

/**
 * @author harsha
 *
 */
public class AppConstants {
	
	
//	Constant to hold intermediate directory path. 
	public static final String PREPROCESSING_OUTPUT = "/colbyrow/preprocess";
	
//	ALPHA and (1 - ALPHA) value for new Page Rank calculation
	public static final Double ALPHA_VALUE = 0.15;

	public static final Double INVERSE_ALPHA_VALUE = 0.85;
	
////	Total number of unique pages. |V|
//	public static Long totalPages = 0L;
//	
//	public static boolean isFirstIteration = true;

//	Holds the dangling factor for the (i + 1)-th iteration.
	public static enum COUNTERS {
		DANGLING_FACTOR, TOTAL_NODES
	}
	
//	Number of runs to refine the Page Rank
	public static final int MAX_RUNS = 10;

	public static final String INTERMEDIATE_OUTPUT = "/colbyrow/pagerank/iteration-";

	public static final String TOP100 = "/colbyrow/top100";

	public static final String DANGLING_NODE_CONTRIBUTION = "D";

	public static final String PAGE_RANK_CONTRIBUTION = "PR";

}
