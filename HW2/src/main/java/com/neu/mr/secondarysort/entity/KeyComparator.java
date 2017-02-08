/**
 * 
 */
package com.neu.mr.secondarysort.entity;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @author harsha
 *
 * Controls how the keys are sorted	before they are passed to reducer
 */
public class KeyComparator extends WritableComparator {
	
	protected KeyComparator() {
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
		
		Integer year1 = Integer.valueOf(stationYearKey1.getYear().get());
		Integer year2 = Integer.valueOf(stationYearKey2.getYear().get());
		
		int cmpVal = stationId1.compareTo(stationId2);
		if(cmpVal == 0){
			return year1.compareTo(year2);
		}
		return cmpVal;
	}
	
	

}
