/**
 * 
 */
package com.neu.mr.rowbycolumn.pagerank;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import com.neu.mr.rowbycolumn.program.PageRankMatrixMultiplicationProgram;


/**
 * @author harsha
 *
 */
public class PageRankJob {

	public static Job configure(Configuration configuration) throws IOException {
		
		Job pageRankJob = new Job(configuration, "PageRankJob");
		pageRankJob.setJarByClass(PageRankMatrixMultiplicationProgram.class);
		
		pageRankJob.setMapperClass(PageRankMapper.class);
		pageRankJob.setMapOutputKeyClass(NullWritable.class);
		pageRankJob.setMapOutputValueClass(Text.class);
		
		pageRankJob.setNumReduceTasks(0);
		
		return pageRankJob;
	}

}
