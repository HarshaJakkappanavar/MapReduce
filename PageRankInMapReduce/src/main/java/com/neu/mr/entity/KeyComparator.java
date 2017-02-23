/**
 * 
 */
package com.neu.mr.entity;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @author harsha
 *
 * Controls how the keys are sorted	before they are passed to reducer
 */
public class KeyComparator extends WritableComparator {
	
	protected KeyComparator() {
		super(Text.class, true);
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.WritableComparator#compare(org.apache.hadoop.io.WritableComparable, org.apache.hadoop.io.WritableComparable)
	 */
	@Override
	public int compare(WritableComparable wC1, WritableComparable wC2) {
		
		Text pageName1 = (Text) wC1;
		
//		All the keys with value as "dangling factor" appears in the end
		
		if("danglingFactor".equals(pageName1)){
			return 1;
		}else{
			return  0;
		}
	}

}
