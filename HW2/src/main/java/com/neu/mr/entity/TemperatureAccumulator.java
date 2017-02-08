/**
 * 
 */
package com.neu.mr.entity;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import com.neu.mr.constants.AppConstants;

/**
 * @author harsha
 *
 */
public class TemperatureAccumulator implements Writable {
	
//	temperatureType = 0 if the type is TMIN
//	temperatureType = 1 if the type is TMAX
	private IntWritable temperatureType;
	
//	Holds the temperature value
	private IntWritable temperature;
	
//	Holds the count of temperatures accumulated
	private IntWritable countSoFar;
	
	public TemperatureAccumulator(){
		this.temperatureType = new IntWritable(0);
		this.temperature = new IntWritable(0);
		this.countSoFar = new IntWritable(0);
	}
	public TemperatureAccumulator(String temperatureType, int temperature) {
		this.temperatureType = new IntWritable(temperatureType.equalsIgnoreCase(
												AppConstants.TMAX_TEXT)?
														AppConstants.TMAX_VALUE:
															AppConstants.TMIN_VALUE);
		this.temperature = new IntWritable(temperature);
		this.countSoFar = new IntWritable(1);
	}
	
	/**
	 * @return the temperatureType
	 */
	public int getTemperatureType() {
		return temperatureType.get();
	}

	/**
	 * @param temperatureType the temperatureType to set
	 */
	public void setTemperatureType(int temperatureType) {
		this.temperatureType.set(temperatureType);
	}

	/**
	 * @return the temperature
	 */
	public int getTemperature() {
		return temperature.get();
	}

	/**
	 * @param temperature the temperature to set
	 */
	public void setTemperature(IntWritable temperature) {
		this.temperature = temperature;
	}
	
	/**
	 * @param temperature the temperature to set
	 */
	public void updateTemperature(int temperature) {
		this.temperature.set(this.temperature.get() + temperature);
	}
	
	/**
	 * @return the countSoFar
	 */
	public IntWritable getCountSoFar() {
		return countSoFar;
	}
	
	/**
	 * @param countSoFar the countSoFar to set
	 */
	public void setCountSoFar(IntWritable countSoFar) {
		this.countSoFar = countSoFar;
	}
	
	/**
	 * @param countSoFar the countSoFar to set
	 */
	public void updateCountSoFar(IntWritable countSoFar) {
		this.countSoFar.set(this.countSoFar.get() + countSoFar.get());
	}
	
	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		this.temperatureType.readFields(in);
		this.temperature.readFields(in);
		this.countSoFar.readFields(in);

	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		this.temperatureType.write(out);
		this.temperature.write(out);
		this.countSoFar.write(out);
	}

}
