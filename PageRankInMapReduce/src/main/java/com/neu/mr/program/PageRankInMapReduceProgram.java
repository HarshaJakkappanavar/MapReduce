/**
 * 
 */
package com.neu.mr.program;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.neu.mr.constants.AppConstants;
import com.neu.mr.entity.PageRankEntity;
import com.neu.mr.entity.PageRankGenericWritable;
import com.neu.mr.pagerank.PageRankMapper;
import com.neu.mr.pagerank.PageRankReducer;
import com.neu.mr.preprocesor.GroupComparator;
import com.neu.mr.preprocesor.HashPartitioner;
import com.neu.mr.preprocesor.KeyComparator;
import com.neu.mr.preprocesor.PreProcessingMapper;
import com.neu.mr.preprocesor.PreProcessingReducer;
import com.neu.mr.topk.TopKMapper;
import com.neu.mr.topk.TopKReducer;

/**
 * @author harsha
 *
 * This is the main run class. The execution is divided into 3 jobs.
 * 1. Pre-processing Job
 * 2. Page Rank calculation and refining Job
 * 3. Fishing out the Top-100 pages with highest page ranks job.
 */
public class PageRankInMapReduceProgram {

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception{

		Configuration configuration = new Configuration();
		String[] cmdLineArgs = new GenericOptionsParser(configuration, args).getRemainingArgs();
		if(cmdLineArgs.length < 2) {
			System.err.println("Usage: hadoop jar <NameofJar.jar> <JobName> <InputDirectory> <OutputDirectory>");
			System.exit(2);
		}
		String outputPath = cmdLineArgs[cmdLineArgs.length - 1];
		
//		START of PREPROCESSING JOB
		Job preProcessingJob = preparePreProcessingJob(configuration);
		
		for(int i = 0; i < cmdLineArgs.length - 1; i++){
			FileInputFormat.addInputPath(preProcessingJob, new Path(cmdLineArgs[i]));
		}
		FileOutputFormat.setOutputPath(preProcessingJob, new Path(cmdLineArgs[cmdLineArgs.length - 1] + AppConstants.INTERMEDIATE_OUTPUT + "1"));
		
		boolean firstJobStatus = preProcessingJob.waitForCompletion(true);
		if(!firstJobStatus)
			throw new Exception("Preprocessing job failed.");
		

//		Get the total pages found based on the number of reduce input records processed and initialize variables onto the configuration.
		Long totalPages = preProcessingJob.getCounters().findCounter("org.apache.hadoop.mapred.Task$Counter", "REDUCE_OUTPUT_RECORDS").getValue();
		configuration.set("totalPages", totalPages.toString());
		configuration.set("isFirstIteration", "true");
		configuration.set("danglingFactor", "0.0");
//		END of PREPROCESSING JOB

		boolean secondJobStatus = false;

//		START of PAGES RANK JOB
		for(int i = 1; i <= AppConstants.MAX_RUNS; i++){
		
			Job pageRankJob = preparePageRankJob(configuration);
			
			FileInputFormat.addInputPath(pageRankJob, new Path(outputPath + AppConstants.INTERMEDIATE_OUTPUT + i));
			FileOutputFormat.setOutputPath(pageRankJob, new Path(outputPath + AppConstants.INTERMEDIATE_OUTPUT + (i+1)));
			
			secondJobStatus = pageRankJob.waitForCompletion(true);
			if(!secondJobStatus)
				throw new Exception("Page Rank processing job failed on the " + i + " iteration.");
			
			configuration.set("isFirstIteration", "false");
			
//			Using counters to set the dangling factor for the (i + 1)th iteration.
			Counters counters = pageRankJob.getCounters();
			
//			Update the dangling factor calculated at the i-th step and update it before the (i+1)-th step.
			Long nextDanglingFactorLongValue =counters.findCounter(AppConstants.DANGLING_FACTOR_ENUM.NEXT_DANGLING_FACTOR).getValue();
			Double nextDanglingFactor = Double.longBitsToDouble(nextDanglingFactorLongValue);
			configuration.set("danglingFactor", nextDanglingFactor.toString());
		}
//		END of PAGE RANK JOB
		
//		START of TOP-100 JOB
		
		Job topKJob = prepareTopKJob(configuration);
		
		FileInputFormat.addInputPath(topKJob, new Path(outputPath + AppConstants.INTERMEDIATE_OUTPUT + (AppConstants.MAX_RUNS + 1)));
		FileOutputFormat.setOutputPath(topKJob, new Path(outputPath + "/top100"));
		
		boolean thirdJobStatus = topKJob.waitForCompletion(true);
//		END of TOP-100 JOB
		
		System.exit(firstJobStatus&&secondJobStatus&&thirdJobStatus?0:1);

	}

	/**
	 * @param configuration
	 * @return
	 * @throws IOException
	 */
	private static Job prepareTopKJob(Configuration configuration) throws IOException {
		Job topKJob = new Job(configuration, "TopKJob");
		topKJob.setJarByClass(PageRankInMapReduceProgram.class);
		topKJob.setMapperClass(TopKMapper.class);
		topKJob.setReducerClass(TopKReducer.class);
		topKJob.setMapOutputKeyClass(NullWritable.class);
		topKJob.setMapOutputValueClass(PageRankEntity.class);
		topKJob.setOutputKeyClass(NullWritable.class);
		topKJob.setOutputValueClass(Text.class);
		
		return topKJob;
	}

	/**
	 * @param configuration
	 * @param cmdLineArgs 
	 * @return
	 * @throws Exception
	 */
	private static Job preparePreProcessingJob(Configuration configuration) throws Exception {
		Job preProcessingJob = new Job(configuration, "PreProcessingJob");
		preProcessingJob.setJarByClass(PageRankInMapReduceProgram.class);
		
		preProcessingJob.setMapperClass(PreProcessingMapper.class);
		preProcessingJob.setMapOutputKeyClass(PageRankEntity.class);
		preProcessingJob.setMapOutputValueClass(PageRankEntity.class);
		
		
		preProcessingJob.setReducerClass(PreProcessingReducer.class);
		preProcessingJob.setOutputKeyClass(NullWritable.class);
		preProcessingJob.setOutputValueClass(Text.class);


		preProcessingJob.setPartitionerClass(HashPartitioner.class);
		preProcessingJob.setSortComparatorClass(KeyComparator.class);
		preProcessingJob.setGroupingComparatorClass(GroupComparator.class);
		
		return preProcessingJob;
	}
	
	/**
	 * @param configuration
	 * @return
	 * @throws IOException 
	 */
	private static Job preparePageRankJob(Configuration configuration) throws IOException {
		
		Job pageRankJob = new Job(configuration, "PageRankJob");
		pageRankJob.setJarByClass(PageRankInMapReduceProgram.class);
		
		pageRankJob.setMapperClass(PageRankMapper.class);
		pageRankJob.setMapOutputKeyClass(Text.class);
		pageRankJob.setMapOutputValueClass(PageRankGenericWritable.class);
		
		pageRankJob.setReducerClass(PageRankReducer.class);
		pageRankJob.setOutputKeyClass(NullWritable.class);
		pageRankJob.setOutputValueClass(Text.class);
		
		return pageRankJob;
	}

}
