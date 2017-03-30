/**
 * 
 */
package com.neu.mr.columnbyrow.topk;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import com.neu.mr.columnbyrow.program.PageRankMatrixMultiplicationProgram;
import com.neu.mr.entity.PageRankEntity;

/**
 * @author harsha
 *
 */
public class TopKJob {

	public static Job configure(Configuration configuration) throws IOException {
		
		Job topKJob = new Job(configuration, "TopKJob");
		topKJob.setJarByClass(PageRankMatrixMultiplicationProgram.class);
		
		topKJob.setMapperClass(TopKMapper.class);
		topKJob.setMapOutputKeyClass(NullWritable.class);
		topKJob.setMapOutputValueClass(Text.class);

		topKJob.setReducerClass(TopKReducer.class);
		topKJob.setOutputKeyClass(NullWritable.class);
		topKJob.setOutputValueClass(Text.class);
		
		return topKJob;
	}

}
