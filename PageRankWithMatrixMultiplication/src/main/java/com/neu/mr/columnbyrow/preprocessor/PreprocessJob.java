/**
 * 
 */
package com.neu.mr.columnbyrow.preprocessor;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.neu.mr.columnbyrow.program.PageRankMatrixMultiplicationProgram;
import com.neu.mr.constants.AppConstants;
import com.neu.mr.entity.PageRankEntity;

/**
 * @author harsha
 *
 */
public class PreprocessJob {

	public static Job configure(Configuration configuration, String[] cmdLineArgs) throws IOException {
		
		Job preProcessingJob = new Job(configuration, "PreProcessingJob");
		preProcessingJob.setJarByClass(PageRankMatrixMultiplicationProgram.class);
		
		preProcessingJob.setMapperClass(PreProcessingMapper.class);
		preProcessingJob.setMapOutputKeyClass(PageRankEntity.class);
		preProcessingJob.setMapOutputValueClass(PageRankEntity.class);
		
		
		preProcessingJob.setReducerClass(PreProcessingReducer.class);
		preProcessingJob.setOutputKeyClass(NullWritable.class);
		preProcessingJob.setOutputValueClass(Text.class);


		preProcessingJob.setPartitionerClass(HashPartitioner.class);
		preProcessingJob.setSortComparatorClass(KeyComparator.class);
		preProcessingJob.setGroupingComparatorClass(GroupComparator.class);

		for(int i = 0; i < cmdLineArgs.length - 1; i++){
			FileInputFormat.addInputPath(preProcessingJob, new Path(cmdLineArgs[i]));
		}
		FileOutputFormat.setOutputPath(preProcessingJob, new Path(cmdLineArgs[cmdLineArgs.length - 1] + AppConstants.PREPROCESSING_OUTPUT));
		
		MultipleOutputs.addNamedOutput(preProcessingJob, "M", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(preProcessingJob, "D", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(preProcessingJob, "R", TextOutputFormat.class, NullWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(preProcessingJob, "pageMap", TextOutputFormat.class, Text.class, Text.class);
		
		preProcessingJob.setNumReduceTasks(1);
		return preProcessingJob;
	
	}

}
