/**
 * 
 */
package com.neu.mr.preprocesor;

import org.apache.hadoop.mapreduce.Partitioner;

import com.neu.mr.entity.PageRankEntity;

/**
 * @author harsha
 * Partiotions the intermediate keys by station id using hashcode.
 */
public class HashPartitioner extends Partitioner<PageRankEntity, PageRankEntity> {

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Partitioner#getPartition(java.lang.Object, java.lang.Object, int)
	 */
	@Override
	public int getPartition(PageRankEntity key, PageRankEntity value, int numPartitions) {
		return (key.getPageName().hashCode() & Integer.MAX_VALUE) % numPartitions;
	}

	
}
