/**
 * 
 */
package com.neu.mr.rowbycolumn.pagerank;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import com.neu.mr.constants.AppConstants;
import com.neu.mr.rowbycolumn.program.PageRankMatrixMultiplicationProgram;

/**
 * @author harsha
 *
 */
public class DanglingFactorJob {

	public static Job configure(Configuration configuration) throws IOException {
		Job danglingFactorJob = new Job(configuration, "DanglingFactorJob");
		danglingFactorJob.setJarByClass(PageRankMatrixMultiplicationProgram.class);
		
		danglingFactorJob.setMapperClass(DanglingNodeMapper.class);
		danglingFactorJob.setMapOutputKeyClass(NullWritable.class);
		danglingFactorJob.setMapOutputValueClass(NullWritable.class);
		
		danglingFactorJob.setNumReduceTasks(0);
		
		return danglingFactorJob;
	}

}
