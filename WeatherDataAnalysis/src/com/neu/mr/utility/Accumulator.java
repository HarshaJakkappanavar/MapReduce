/**
 * 
 */
package com.neu.mr.utility;

import java.util.List;
import java.util.Map;

import com.neu.mr.constants.AppConstants;
import com.neu.mr.entity.StationDataAccumulator;

/**
 * This is a utility class that updates the accumulation datastructure.
 * 
 * @author harsha
 *
 */
public class Accumulator {
	
	/**
	 * @param lines
	 * @param startIndex
	 * @param endIndex
	 * @param accumulator
	 */
	public static void populateAccumulator(List<String> lines, int startIndex, int endIndex, Map<String, StationDataAccumulator> accumulator){
		
		for(int i = startIndex; i < endIndex; i++){
			String line = lines.get(i);
			String[] lineArray = line.split(",");
//			if(lineArray.length < 4)
//				continue;
			String tmaxText = lineArray[2].trim();
			if(tmaxText.equalsIgnoreCase(AppConstants.TMAX_TEXT)){
				String stationId = lineArray[0];
				
				int tmax = Integer.parseInt(lineArray[3]);
				
				StationDataAccumulator stationDataAccumulator = accumulator.get(stationId);
				if(null == stationDataAccumulator){
					stationDataAccumulator = new StationDataAccumulator();
				}
				stationDataAccumulator.updateSumSoFar(tmax);
				/*
				 * Making the computation task expensive by calling Fibonacci(17)
				 */
				if(AppConstants.IS_EXPENSIVE)
					findFibonacci(AppConstants.FIBONACCI_NUMBER_TO_MAKE_EXECUTION_EXPENSIVE);
				stationDataAccumulator.updateCountSoFar();
				stationDataAccumulator.computeAvg();
				
				accumulator.put(stationId, stationDataAccumulator);
			}
			
		}
	}

	/**
	 * @param lines
	 * @param startIndex
	 * @param endIndex
	 */
	public static void populateCoarseLockAccumulator(List<String> lines, int startIndex, int endIndex){
		
		for(int i = startIndex; i < endIndex; i++){
			String line = lines.get(i);
			String[] lineArray = line.split(",");
//			if(lineArray.length < 4)
//				continue;
			String tmaxText = lineArray[2].trim();
			if(tmaxText.equalsIgnoreCase(AppConstants.TMAX_TEXT)){
				String stationId = lineArray[0];
				
				int tmax = Integer.parseInt(lineArray[3]);
				
				synchronized (AppConstants.accumulator) {
					
					StationDataAccumulator stationDataAccumulator = AppConstants.accumulator.get(stationId);
					if(null == stationDataAccumulator){
						stationDataAccumulator = new StationDataAccumulator();
					}
					stationDataAccumulator.updateSumSoFar(tmax);
					/*
					 * Making the computation task expensive by calling Fibonacci(17)
					 */
					if(AppConstants.IS_EXPENSIVE)
						findFibonacci(AppConstants.FIBONACCI_NUMBER_TO_MAKE_EXECUTION_EXPENSIVE);
					stationDataAccumulator.updateCountSoFar();
					stationDataAccumulator.computeAvg();
					
					AppConstants.accumulator.put(stationId, stationDataAccumulator);
				}
			}
			
		}
	}

	/**
	 * @param lines
	 * @param startIndex
	 * @param endIndex
	 */
	public static void populateFineLockAccumulator(List<String> lines, int startIndex, int endIndex){
		
		for(int i = startIndex; i < endIndex; i++){
			String line = lines.get(i);
			String[] lineArray = line.split(",");
//			if(lineArray.length < 4)
//				continue;
			String tmaxText = lineArray[2].trim();
			if(tmaxText.equalsIgnoreCase(AppConstants.TMAX_TEXT)){
				String stationId = lineArray[0];
				
				int tmax = Integer.parseInt(lineArray[3]);
				
				StationDataAccumulator stationDataAccumulator = AppConstants.accumulator.get(stationId);
				if(null == stationDataAccumulator){
					stationDataAccumulator = new StationDataAccumulator();
				}
				
				synchronized (stationDataAccumulator) {
					stationDataAccumulator.updateSumSoFar(tmax);
					
					if(AppConstants.IS_EXPENSIVE)
						findFibonacci(AppConstants.FIBONACCI_NUMBER_TO_MAKE_EXECUTION_EXPENSIVE);
					stationDataAccumulator.updateCountSoFar();
					stationDataAccumulator.computeAvg();
					
					AppConstants.accumulator.put(stationId, stationDataAccumulator);
				}
			}
			
		}
	}
	
	/**
	 * @param n
	 * @return
	 */
	public static int findFibonacci(int n){
		
		
		if(n < 0)
			return -1;
		
		if(n == 0)
			return 0;
			
		if(n == 1)
			return 1;
		
		else 
			return findFibonacci(n - 1) + findFibonacci(n - 2);
	}

}
