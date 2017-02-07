/**
 * 
 */
package com.neu.mr.entity;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

/**
 * @author harsha
 *
 */
public class MeanTemperatureAccumulator implements Writable {
	
//	Accumulates the sum of temperatures
	private IntWritable sumSoFar;
	
//	Accumulates the count of temperatures
	private IntWritable countSoFar;
	
	public MeanTemperatureAccumulator() {
		this.sumSoFar = new IntWritable(0);
		this.countSoFar = new IntWritable(0);
	}
	
	/**
	 * @return the sumSoFar
	 */
	public IntWritable getSumSoFar() {
		return sumSoFar;
	}

	/**
	 * Updates the attribute with the passed argument
	 * @param sumSoFar the sumSoFar to set
	 */
	public void updateSumSoFar(int sumSoFar) {
		this.sumSoFar.set(this.sumSoFar.get() + sumSoFar);
	}

	/**
	 * @return the countSoFar
	 */
	public IntWritable getCountSoFar() {
		return countSoFar;
	}

	/**
	 * Updates the attribute by 1
	 * @param countSoFar the countSoFar to set
	 */
	public void updateCountSoFar() {
		this.countSoFar.set(this.countSoFar.get()+1);
	}

	public void updateCountSoFar(int count) {
		this.countSoFar.set(this.countSoFar.get() + count);
		
	}
	

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return this.countSoFar.get() == 0?"None":String.valueOf(this.sumSoFar.get()/(double) this.countSoFar.get());
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		this.sumSoFar.set(in.readInt());
		this.countSoFar.set(in.readInt());

	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(sumSoFar.get());
		out.writeInt(countSoFar.get());
	}

}
