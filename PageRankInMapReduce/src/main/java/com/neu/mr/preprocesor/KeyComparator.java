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
 * Controls how the keys are sorted	before they are passed to reducer.
 * The value object is sorted such that the first object in the Iterable object "value", is always the real PageRankEntity object with content. 
 */
public class KeyComparator extends WritableComparator {
	
	protected KeyComparator() {
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
		
		Integer outlinkSize1 = Integer.valueOf(pageRankEntity1.getOutlinkSize());
		Integer outlinkSize2 = Integer.valueOf(pageRankEntity2.getOutlinkSize());
		
		int cmpVal = pageName1.compareTo(pageName2);
		if(cmpVal == 0){
			return outlinkSize2.compareTo(outlinkSize1);
		}
		return cmpVal;
	}
	
	

}
