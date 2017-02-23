/**
 * 
 */
package com.neu.mr.preprocesor;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import com.neu.mr.entity.PageRankEntity;

/**
 * @author harsha
 * 
 * Groups the intermediate keys by station id and ignores the year. This means that all the intermediate keys with the same station id are processed in the same reduce call.
 */
public class GroupComparator extends WritableComparator {

	
	protected GroupComparator() {
		super(PageRankEntity.class, true);
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.WritableComparator#compare(org.apache.hadoop.io.WritableComparable, org.apache.hadoop.io.WritableComparable)
	 */
	@Override
	public int compare(WritableComparable wC1, WritableComparable wC2) {
		PageRankEntity pageRankEntity1 = (PageRankEntity) wC1;
		PageRankEntity pageRankEntity2 = (PageRankEntity) wC2;
		
		String pageName1 = pageRankEntity1.getPageName();
		String pageName2 = pageRankEntity2.getPageName();
		
		return pageName1.compareTo(pageName2);
	}
	
	
}
