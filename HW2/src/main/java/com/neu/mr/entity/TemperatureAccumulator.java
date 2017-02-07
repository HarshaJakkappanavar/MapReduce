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
	
	public TemperatureAccumulator(String temperatureType, int temperature) {
		this.temperatureType = new IntWritable(temperatureType.equalsIgnoreCase(
												AppConstants.TMAX_TEXT)?
														AppConstants.TMAX_VALUE:
															AppConstants.TMIN_VALUE);
		this.temperature = new IntWritable(temperature);
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
	public void setTemperatureType(IntWritable temperatureType) {
		this.temperatureType = temperatureType;
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

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		this.temperatureType.set(in.readInt());
		this.temperature.set(in.readInt());

	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(temperatureType.get());
		out.writeInt(temperature.get());
	}

}
