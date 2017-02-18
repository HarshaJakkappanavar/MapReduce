/**
 * 
 */
package com.neu.mr.secondarysort.entity;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * @author harsha
 * The datastructure holds station id and year as key
 */
public class StationYearKey implements Writable, WritableComparable<StationYearKey> {
	
	private Text stationId;
	
	private IntWritable year;
	
	public StationYearKey() {
		this.stationId = new Text("");
		this.year = new IntWritable(0);
	}
	
	public StationYearKey(String stationId, int year){
		this.stationId = new Text(stationId);
		this.year = new IntWritable(year);
	}
	
	/**
	 * @return the stationId
	 */
	public Text getStationId() {
		return stationId;
	}

	/**
	 * @param stationId the stationId to set
	 */
	public void setStationId(Text stationId) {
		this.stationId = stationId;
	}

	/**
	 * @return the year
	 */
	public IntWritable getYear() {
		return year;
	}

	/**
	 * @param year the year to set
	 */
	public void setYear(IntWritable year) {
		this.year = year;
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((stationId == null) ? 0 : stationId.hashCode());
		result = prime * result + ((year == null) ? 0 : year.hashCode());
		return result;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		StationYearKey other = (StationYearKey) obj;
		if (stationId == null) {
			if (other.stationId != null)
				return false;
		} else if (!stationId.equals(other.stationId))
			return false;
		if (year == null) {
			if (other.year != null)
				return false;
		} else if (!year.equals(other.year))
			return false;
		return true;
	}

	/* (non-Javadoc)
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(StationYearKey stationYearKey) {
		return this.stationId.toString().compareTo(stationYearKey.getStationId().toString());
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		this.stationId.write(out);
		this.year.write(out);

	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		this.stationId.readFields(in);
		this.year.readFields(in);

	}

}
