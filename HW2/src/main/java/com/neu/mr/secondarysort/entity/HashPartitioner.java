/**
 * 
 */
package com.neu.mr.secondarysort.entity;

import org.apache.hadoop.mapreduce.Partitioner;

import com.neu.mr.entity.TemperatureAccumulator;

/**
 * @author harsha
 *
 */
public class HashPartitioner extends Partitioner<StationYearKey, TemperatureAccumulator> {

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Partitioner#getPartition(java.lang.Object, java.lang.Object, int)
	 */
	@Override
	public int getPartition(StationYearKey key, TemperatureAccumulator value, int numPartitions) {
		return (key.getStationId().hashCode() & Integer.MAX_VALUE) % numPartitions;
	}

	
}
