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
import org.apache.hadoop.mapreduce.Job;

import com.neu.mr.columnbyrow.program.PageRankMatrixMultiplicationProgram;

/**
 * @author harsha
 *
 */
public class PageRankJob {

	public static Job configure(Configuration configuration) throws IOException {
		
		Job pageRankJob = new Job(configuration, "PageRankJob");
		pageRankJob.setJarByClass(PageRankMatrixMultiplicationProgram.class);
		
//		pageRankJob.setMapperClass(PageRankMapper.class);
		pageRankJob.setMapOutputKeyClass(LongWritable.class);
		pageRankJob.setMapOutputValueClass(Text.class);
		
		pageRankJob.setReducerClass(PageRankReducer.class);
		pageRankJob.setOutputKeyClass(NullWritable.class);
		pageRankJob.setOutputValueClass(Text.class);
		
		return pageRankJob;
	}

}
