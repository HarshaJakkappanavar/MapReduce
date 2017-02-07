/**
 * 
 */
package com.neu.mr.utility;

import java.util.Map;
import java.util.Map.Entry;

import com.neu.mr.entity.StationDataAccumulator;

/**
 * This is the utility program that renders the output onto the console.
 * 
 * @author harsha
 *
 */
public class WriterRoutine {

	/**
	 * @param accumulator
	 */
	public static void consoleWriter(Map<String, StationDataAccumulator> accumulator){
		
		System.out.println("Accumulator Output:");
		for(Entry<String, StationDataAccumulator> accumulatorEntry : accumulator.entrySet()){
			System.out.println(accumulatorEntry.getKey() + " : " + accumulatorEntry.getValue().getSumSoFar()/(double) accumulatorEntry.getValue().getCountSoFar());
		}
	}

	/**
	 * @param minTime
	 * @param maxTime
	 * @param avgTime
	 */
	public static void writeRunningTimeToConsloe(long minTime, long maxTime, double avgTime) {
		System.out.println("Min Time: " + minTime);
		System.out.println("Max Time: " + maxTime);
		System.out.println("Avg Time: " + avgTime);
		
	}
}
