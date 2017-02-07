/**
 * 
 */
package com.neu.mr.threads;

import java.util.List;

import com.neu.mr.constants.AppConstants;
import com.neu.mr.utility.Accumulator;

/**
 * The thread datastructure to run the NO-LOCK version
 * 
 * @author harsha
 *
 */
public class NoLockThread implements Runnable {
	
	List<String> lines = null;
	
	int startIndex;
	
	int endIndex;
	
	public NoLockThread(List<String> lines, int startIndex, int lastIndex) {
		this.lines = lines;
		this.startIndex = startIndex;
		this.endIndex = lastIndex;
	}

	/* (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {
		Accumulator.populateAccumulator(lines, startIndex, endIndex, AppConstants.accumulator);
	}

}
