/**
 * 
 */
package com.neu.mr.program;

import java.util.HashMap;
import java.util.List;

import com.neu.mr.constants.AppConstants;
import com.neu.mr.entity.StationDataAccumulator;
import com.neu.mr.utility.Accumulator;
import com.neu.mr.utility.LoaderRoutine;
import com.neu.mr.utility.WriterRoutine;

/**
 * The main program that runs the SEQUENTIAL version of execution
 * 
 * @author harsha
 *
 */
public class SequentialProgram {
	
	static long minTime = Long.MAX_VALUE;
	
	static long maxTime = Long.MIN_VALUE;
	
	static long totalTime = 0;
	
	static double avgTime = 0;

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		if(args.length == 0){
			System.out.println("No input file provided. Exiting...");
			System.exit(0);
		}
		
		String inputFileName = args[0];
		if(args.length == 2 && args[1].equalsIgnoreCase("true"))
			AppConstants.IS_EXPENSIVE = true;
		
		List<String> lines = LoaderRoutine.readFile(inputFileName);
		
		int startIndex = 0;
		int endIndex = lines.size();
		
//		Running the execution multiple times
		for(int i = 0; i < AppConstants.MAX_LOOP_RUNS; i++){
			/*
			 * Timing the execution of the program and updating the minimum & maximum running times.
			 */
			long timeBefore = System.currentTimeMillis();
			Accumulator.populateAccumulator(lines, startIndex, endIndex, AppConstants.accumulator);
			long timeAfter = System.currentTimeMillis();
			
			long timeTaken = timeAfter - timeBefore;
			
			minTime = Long.min(timeTaken, minTime);
			maxTime = Long.max(timeTaken, maxTime);
			
			totalTime += timeTaken;
			System.out.println("Time taken for the run: " + (i+1) + " is: " + timeTaken + " milliseconds.\n");
			if(i != AppConstants.MAX_LOOP_RUNS - 1)
				AppConstants.accumulator = new HashMap<String, StationDataAccumulator>();
			
		}
		
		/*
		 * Calculating the average running time of the executions and printing the result to the console.
		 */
		avgTime = totalTime/(double) AppConstants.MAX_LOOP_RUNS;

		WriterRoutine.consoleWriter(AppConstants.accumulator);
		
		WriterRoutine.writeRunningTimeToConsloe(minTime, maxTime, avgTime);
		
		
	}

}
