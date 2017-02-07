/**
 * 
 */
package com.neu.mr.entity;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

/**
 * @author harsha
 *
 */
public class MeanTemperatureOutput implements Writable {

//	Holds the average value for the TMIN
	private MeanTemperatureAccumulator tMinMeanAccumulator;

//	Holds the average value for the TMAX
	private MeanTemperatureAccumulator tMaxMeanAccumulator;
	
	public MeanTemperatureOutput() {
		this.tMaxMeanAccumulator = new MeanTemperatureAccumulator();
		this.tMinMeanAccumulator = new MeanTemperatureAccumulator();
	}
	
	public void updateTMinMeanAccumulator(int temperature, int count){
		this.tMinMeanAccumulator.updateSumSoFar(temperature);
		this.tMinMeanAccumulator.updateCountSoFar(count);
	}
	
	public void updateTMaxMeanAccumulator(int temperature, int count){
		this.tMaxMeanAccumulator.updateSumSoFar(temperature);
		this.tMaxMeanAccumulator.updateCountSoFar(count);
	}
	
	
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return tMinMeanAccumulator.toString() + ", " + tMaxMeanAccumulator.toString();
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		this.tMinMeanAccumulator.readFields(in);
		this.tMaxMeanAccumulator.readFields(in);
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		
		this.tMinMeanAccumulator.write(out);
		this.tMaxMeanAccumulator.write(out);
	}

}
