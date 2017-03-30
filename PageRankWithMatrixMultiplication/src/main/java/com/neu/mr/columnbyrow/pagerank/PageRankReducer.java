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
public class PageRankReducer extends Reducer<LongWritable, Text, NullWritable, Text> {

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
	 */
	@Override
	protected void reduce(LongWritable key, Iterable<Text> value,
			Reducer<LongWritable, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		

		
		Double totalContribution = 0.0;
		Double danglingContribution = 0.0;
		
		for(Text valTuple : value){
			String[] valTupleParts = valTuple.toString().split(":");
			if(valTupleParts[0].equalsIgnoreCase(AppConstants.DANGLING_NODE_CONTRIBUTION)){
				danglingContribution += Double.parseDouble(valTupleParts[1]);
			}else if(valTupleParts[0].equalsIgnoreCase(AppConstants.PAGE_RANK_CONTRIBUTION)) {
				totalContribution += Double.parseDouble(valTupleParts[1]);
			}
		}

		Configuration configuration = context.getConfiguration();
		Long totalNodes = configuration.getLong("TOTAL_NODES", 0L);
		
		Double newPageRank = (AppConstants.ALPHA_VALUE / totalNodes) + AppConstants.INVERSE_ALPHA_VALUE * (totalContribution + danglingContribution);
		context.write(NullWritable.get(), new Text(key.get() + ":" + newPageRank));
		
	}

}
