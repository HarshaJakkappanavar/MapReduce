/**
 * 
 */
package com.neu.mr.columnbyrow.pagerank;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.neu.mr.constants.AppConstants;

/**
 * @author harsha
 *
 */
public class PageRankReducer extends Reducer<LongWritable, DoubleWritable, NullWritable, Text> {

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
	 */
	@Override
	protected void reduce(LongWritable key, Iterable<DoubleWritable> value,
			Reducer<LongWritable, DoubleWritable, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {

		Double totalContribution = 0.0;
		for(DoubleWritable contribution : value){
			totalContribution += contribution.get();
		}

		Configuration configuration = context.getConfiguration();
		Long totalNodes = configuration.getLong("TOTAL_NODES", 0L);
		
		Double newPageRank = (AppConstants.ALPHA_VALUE / totalNodes) + AppConstants.INVERSE_ALPHA_VALUE * totalContribution;
		context.write(NullWritable.get(), new Text(key.get() + ":" + newPageRank));
		
	}

}
