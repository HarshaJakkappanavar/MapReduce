/**
 * 
 */
package com.neu.mr.entity;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.GenericWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * @author harsha
 *
 *	Custom Generic Writable class to emit objects of PageRankEntity, Text, DoubleWritable class from Mapper and collect at the Reducer
 */
public class PageRankGenericWritable extends GenericWritable {
	
	private static Class<? extends Writable>[] CLASSES = null;
	
	static {
		CLASSES = (Class<? extends Writable>[]) new Class[] { 
			PageRankEntity.class,
			Text.class,
			DoubleWritable.class
		};
	}

	public PageRankGenericWritable(){
		
	}
	
	public PageRankGenericWritable(Writable entity){
		set(entity);
	}
	
	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.GenericWritable#getTypes()
	 */
	@Override
	protected Class<? extends Writable>[] getTypes() {
		return CLASSES;
	}

}
