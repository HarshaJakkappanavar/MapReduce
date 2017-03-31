/**
 * 
 */
package com.neu.mr.preprocessor.transpose;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.neu.mr.constants.AppConstants;
import com.neu.mr.rowbycolumn.program.PageRankMatrixMultiplicationProgram;

/**
 * @author harsha
 *
 */
public class TransposeJob {

	public static Job configure(Configuration configuration, String outputPath) throws IllegalArgumentException, IOException {
		Job transposeJob = new Job(configuration, "TransposeJob");
		transposeJob.setJarByClass(PageRankMatrixMultiplicationProgram.class);
		
		transposeJob.setMapperClass(TransposeMapper.class);
		transposeJob.setMapOutputKeyClass(LongWritable.class);
		transposeJob.setMapOutputValueClass(Text.class);
		
		
		transposeJob.setReducerClass(TransposeReducer.class);
		transposeJob.setOutputKeyClass(NullWritable.class);
		transposeJob.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(transposeJob, new Path(outputPath + AppConstants.PREPROCESSING_OUTPUT + "/M-r-00000"));
		FileOutputFormat.setOutputPath(transposeJob, new Path(outputPath + AppConstants.TRANSPOSE_OUTPUT));
		
		return transposeJob;
	}

}
