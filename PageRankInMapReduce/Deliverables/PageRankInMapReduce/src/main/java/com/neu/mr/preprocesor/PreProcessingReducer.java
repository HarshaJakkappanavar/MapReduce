/**
 * 
 */
package com.neu.mr.preprocesor;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.neu.mr.entity.PageRankEntity;

/**
 * @author harsha
 *
 *	Picks the first object in the Iterable "value" and emits it.
 */
public class PreProcessingReducer extends Reducer<PageRankEntity, PageRankEntity, NullWritable, Text> {

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
	 */
	@Override
	protected void reduce(PageRankEntity key, Iterable<PageRankEntity> value, Reducer<PageRankEntity, PageRankEntity, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		
		PageRankEntity pageRankEntity = null;
		
		for(PageRankEntity valueEntity : value){
			pageRankEntity = valueEntity;
			break;
		}
		
		if(null != pageRankEntity){
			context.write(NullWritable.get(), new Text(pageRankEntity.toString()));
		}
		
	}
}
