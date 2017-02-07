/**
 * 
 */
package com.neu.mr.constants;

import java.util.HashMap;
import java.util.Map;

import com.neu.mr.entity.StationDataAccumulator;

/**
 * This class holds all the application related Constants and Variables.
 * 
 * @author harsha
 *
 */
public class AppConstants {
	
//	Constant string to hold the TMAX text
	public static final String TMAX_TEXT = "TMAX";
	
//	The maximum number of threads for parallel execution
	public static final int MAX_NO_OF_THREADS = 4;

	public static final int MAX_LOOP_RUNS = 10;

	public static final int FIBONACCI_NUMBER_TO_MAKE_EXECUTION_EXPENSIVE = 17;
	
	public static int SUB_LINES_COUNT = 0;
	
	public static boolean IS_EXPENSIVE = false;
	
//	The accumulation datastructure
	public static Map<String, StationDataAccumulator> accumulator = new HashMap<String, StationDataAccumulator>();

}
