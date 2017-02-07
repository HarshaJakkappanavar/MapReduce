/**
 * 
 */
package com.neu.mr.entity;

/**
 * This data structure holds the running sum, running count and temperature average for a particular stationId.
 * 
 * @author harsha
 *
 */
public class StationDataAccumulator {

	private int sumSoFar;
	
	private int countSoFar;
	
	private double avg;
	
	public StationDataAccumulator() {
		this.sumSoFar = 0;
		this.countSoFar = 0;
		this.avg = 0;
	}
	
	/**
	 * @return the sumSoFar
	 */
	public int getSumSoFar() {
		return sumSoFar;
	}

	/**
	 * Updates the attribute with the passed argument
	 * @param sumSoFar the sumSoFar to set
	 */
	public void updateSumSoFar(int sumSoFar) {
		this.sumSoFar += sumSoFar;
	}

	/**
	 * @return the countSoFar
	 */
	public int getCountSoFar() {
		return countSoFar;
	}

	/**
	 * Updates the attribute by 1
	 * @param countSoFar the countSoFar to set
	 */
	public void updateCountSoFar() {
		this.countSoFar++;
	}

	public void updateCountSoFar(int count) {
		this.countSoFar += count;
		
	}
	
	public void computeAvg(){
		this.avg = this.sumSoFar/(double) this.countSoFar;
	}
	
	public double getAvg(){
		return this.avg;
	}
	
}
