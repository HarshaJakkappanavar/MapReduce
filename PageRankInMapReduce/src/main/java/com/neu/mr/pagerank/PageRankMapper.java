/**
 * 
 */
package com.neu.mr.pagerank;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.neu.mr.constants.AppConstants;
import com.neu.mr.entity.PageRankEntity;
import com.neu.mr.entity.PageRankGenericWritable;
import com.neu.mr.utility.PageRankUtility;

/**
 * @author harsha
 *
 *	- Reads the intermediate output from the Preprocessing job. 
 *	- Creates a PageRankEntity object.
 *	- Emits the object
 *	- if this page has no outlinks, then the page rank of this page is contributed towards the dangling factor calculation.
 *	- else the page rank of this page is passed on to all the outward linked pages.
 */
public class PageRankMapper extends Mapper<LongWritable, Text, Text, PageRankGenericWritable> {

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Mapper#map(java.lang.Object, java.lang.Object, org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, PageRankGenericWritable>.Context context)
			throws IOException, InterruptedException {
		
		Configuration configuration = context.getConfiguration();
		
		AppConstants.isFirstIteration = configuration.getBoolean("isFirstIteration", true);
		
//		Create a Page rank datatype from the input line.
		PageRankEntity pageRankEntity = PageRankUtility.getPageRankEntityFromValue(value);
		
//		emit the page rank object having the outlinks in it.
		context.write(new Text(pageRankEntity.getPageName()), new PageRankGenericWritable(pageRankEntity));
		
		
//		if there are no outlinks, then this page qualifies to be a dangling node 
//		and hence I accumulates the page rank by way of order inversion for next iteration..
		if(pageRankEntity.getOutlinkSize() == 0){
			DoubleWritable danglingPageRank = pageRankEntity.getPageRank();
			context.write(new Text("danglingFactorReduce"), new PageRankGenericWritable(danglingPageRank));
		}else{
//			emit the page rank contribution.
			Double pageRankDouble = pageRankEntity.getPageRank().get() / (double) pageRankEntity.getOutlinkSize();
			DoubleWritable pageRank = new DoubleWritable(pageRankDouble);
			for(Text outlink : pageRankEntity.getOutlinks()){
				context.write(outlink, new PageRankGenericWritable(pageRank));
			}
		}
		
	}

}
