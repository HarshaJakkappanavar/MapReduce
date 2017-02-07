/**
 * 
 */
package com.neu.mr.threads;

import java.util.List;

import com.neu.mr.utility.Accumulator;

/**
 * The thread datastructure to run the COARSE-LOCK version
 * 
 * @author harsha
 *
 */
public class CoarseLockThread implements Runnable {
	
	List<String> lines = null;
	
	int startIndex;
	
	int endIndex;
	
	public CoarseLockThread(List<String> lines, int startIndex, int lastIndex) {
		this.lines = lines;
		this.startIndex = startIndex;
		this.endIndex = lastIndex;
	}

	/* (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {
		Accumulator.populateCoarseLockAccumulator(lines, startIndex, endIndex);
	}

}
