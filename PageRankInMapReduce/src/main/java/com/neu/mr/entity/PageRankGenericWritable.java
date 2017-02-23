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
