/**
 * 
 */
package com.neu.mr.pagerank;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import com.neu.mr.constants.AppConstants;
import com.neu.mr.entity.PageRankEntity;
import com.neu.mr.entity.PageRankGenericWritable;

/**
 * @author harsha
 *
 *	- Collects the inlinks to this page.
 *	- If the key is "danglingFactorReduce", then all the page rank contributions of all the dangling nodes are collected and set to the counter.
 *	- else page rank from the inlinks are added and a new page rank is calculated and updated.
 */
public class PageRankReducer extends Reducer<Text, PageRankGenericWritable, NullWritable, Text> {

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
	 */
	@Override
	protected void reduce(Text key, Iterable<PageRankGenericWritable> value,
			Reducer<Text, PageRankGenericWritable, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		
//		If the key is a page name and not "danglingFactor" then update the page rank and emit
		if(!"danglingFactorReduce".equals(key.toString())){
			
			Double inlinksPageRankSummation = 0.0;
			PageRankEntity pageRankEntity = null;
			
			for(PageRankGenericWritable genericWritable : value){
				Writable valueObj = genericWritable.get();
				if(valueObj instanceof PageRankEntity){
//					get the page rank object
					pageRankEntity = (PageRankEntity) valueObj;
				}else if (valueObj instanceof DoubleWritable){
					
//					accumulate the page rank contributions from the inlinks
					DoubleWritable pageRank = (DoubleWritable) valueObj;
					inlinksPageRankSummation += pageRank.get();
				}
			}
			
			Configuration configuration = context.getConfiguration();
			AppConstants.totalPages = configuration.getLong("totalPages", 0L);
			Double danglingFactor = configuration.getDouble("danglingFactor", 0.0);
			
//			calculate the dangling factor value
			Double danglingFactorContribution = danglingFactor / AppConstants.totalPages.doubleValue();
			
//			calculate the new page rank
			Double newPageRank = (AppConstants.ALPHA_VALUE / AppConstants.totalPages.doubleValue())
					+ (AppConstants.INVERSE_ALPHA_VALUE * (danglingFactorContribution + inlinksPageRankSummation));
			
			pageRankEntity.setPageRank(new DoubleWritable(newPageRank));
			
			context.write(NullWritable.get(), new Text(pageRankEntity.toString()));
			
		}else{
//			accumulate the dangling factor for this iteration and set it to the configuration, which will be used on the next iteration.
			Double runningDanglingFactor = 0.0;
			
			for(PageRankGenericWritable genericWritable : value){
				Writable valueObj = genericWritable.get();
				if(valueObj instanceof DoubleWritable){
					DoubleWritable danglingPageRank = (DoubleWritable) valueObj;
					runningDanglingFactor += danglingPageRank.get();
				}
			}
			
			context.getCounter(AppConstants.DANGLING_FACTOR_ENUM.NEXT_DANGLING_FACTOR).setValue(Double.doubleToLongBits(runningDanglingFactor));
		}
	}

	
	
}
