/**
 * 
 */
package com.neu.mr.threads;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.neu.mr.entity.StationDataAccumulator;
import com.neu.mr.utility.Accumulator;

/**
 * The thread datastructure to run the NO-SHARING version
 * 
 * @author harsha
 *
 */
public class NoSharingThread implements Runnable {
	
	List<String> lines = null;
	
	int startIndex;
	
	int endIndex;
	
	Map<String, StationDataAccumulator> accumulator = null;
	
	public NoSharingThread(List<String> lines, int startIndex, int endIndex) {
		this.lines = lines;
		this.startIndex = startIndex;
		this.endIndex = endIndex;
		accumulator = new HashMap<String, StationDataAccumulator>();
	}
	
	public Map<String, StationDataAccumulator> getAccumulator(){
		return accumulator;
	}

	/* (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {
		Accumulator.populateAccumulator(lines, startIndex, endIndex, accumulator);
	}

}
