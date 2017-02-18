/**
 * 
 */
package com.neu.mr.secondarysort.entity;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @author harsha
 * 
 * Groups the intermediate keys by station id and ignores the year. This means that all the intermediate keys with the same station id are processed in the same reduce call.
 */
public class GroupComparator extends WritableComparator {

	
	protected GroupComparator() {
		super(StationYearKey.class, true);
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.WritableComparator#compare(org.apache.hadoop.io.WritableComparable, org.apache.hadoop.io.WritableComparable)
	 */
	@Override
	public int compare(WritableComparable wC1, WritableComparable wC2) {
		StationYearKey stationYearKey1 = (StationYearKey) wC1;
		StationYearKey stationYearKey2 = (StationYearKey) wC2;
		
		String stationId1 = stationYearKey1.getStationId().toString();
		String stationId2 = stationYearKey2.getStationId().toString();
		
		return stationId1.compareTo(stationId2);
	}
	
	
}
