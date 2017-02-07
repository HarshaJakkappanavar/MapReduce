/**
 * 
 */
package com.neu.mr.utility;

import java.util.Map;
import java.util.Map.Entry;

import com.neu.mr.constants.AppConstants;
import com.neu.mr.entity.StationDataAccumulator;

/**
 * This is a utility datastructure that helps in combining the accumulation datastructures of the individual threads from NO-SHARING version of execution.
 * 
 * @author harsha
 *
 */
public class CombinerRoutine {
	
	/**
	 * @param noSharingAccumulator
	 */
	public static void combineWithGlobalAccumulator(Map<String, StationDataAccumulator> noSharingAccumulator){
		
		for(Entry<String, StationDataAccumulator> accumulatorEntry : noSharingAccumulator.entrySet()){
			StationDataAccumulator stationDataAccumulator = AppConstants.accumulator.get(accumulatorEntry.getKey());
			if(null == stationDataAccumulator){
				stationDataAccumulator = new StationDataAccumulator();
			}
			stationDataAccumulator.updateSumSoFar(accumulatorEntry.getValue().getSumSoFar());
			/*
			 * Making the computation task expensive by calling Fibonacci(17)
			 */
			if(AppConstants.IS_EXPENSIVE)
				Accumulator.findFibonacci(AppConstants.FIBONACCI_NUMBER_TO_MAKE_EXECUTION_EXPENSIVE);
			stationDataAccumulator.updateCountSoFar(accumulatorEntry.getValue().getCountSoFar());
			stationDataAccumulator.computeAvg();
			
			AppConstants.accumulator.put(accumulatorEntry.getKey(), stationDataAccumulator);
		}
	}

}
