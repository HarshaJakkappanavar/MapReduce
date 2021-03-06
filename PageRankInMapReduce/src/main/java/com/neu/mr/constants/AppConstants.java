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
	public static final String INTERMEDIATE_OUTPUT = "/intermediate_";
	
//	ALPHA and (1 - ALPHA) value for new Page Rank calculation
	public static final Double ALPHA_VALUE = 0.15;

	public static final Double INVERSE_ALPHA_VALUE = 0.85;
	
////	Total number of unique pages. |V|
//	public static Long totalPages = 0L;
//	
//	public static boolean isFirstIteration = true;

//	Holds the dangling factor for the (i + 1)-th iteration.
	public static enum DANGLING_FACTOR_ENUM {
		NEXT_DANGLING_FACTOR
	}
	
//	Number of runs to refine the Page Rank
	public static final int MAX_RUNS = 10;

}
