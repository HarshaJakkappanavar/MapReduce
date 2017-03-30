/**
 * 
 */
package com.neu.mr.columnbyrow.program;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.neu.mr.columnbyrow.pagerank.PageRankJob;
import com.neu.mr.columnbyrow.preprocessor.PreprocessJob;
import com.neu.mr.columnbyrow.topk.TopKJob;
import com.neu.mr.constants.AppConstants;

/**
 * @author harsha
 *
 * This is the main run class. The execution is divided into 3 jobs.
 * 1. Pre-processing Job
 * 2. Page Rank calculation and refining Job
 * 3. Fishing out the Top-100 pages with highest page ranks job.
 * 
 * This program demonstrates the matrix multiplication as column by row (outlinks in rows).
 */
public class PageRankMatrixMultiplicationProgram {

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		
		Configuration configuration = new Configuration();
		String[] cmdLineArgs = new GenericOptionsParser(configuration, args).getRemainingArgs();
		if(cmdLineArgs.length < 2) {
			System.err.println("Usage: hadoop jar <NameofJar.jar> <JobName> <InputDirectory> <OutputDirectory>");
			System.exit(2);
		}
		String outputPath = cmdLineArgs[cmdLineArgs.length - 1];
		
//		Start of PREPROCESSING JOB
		Job preprocessingJob = PreprocessJob.configure(configuration, cmdLineArgs);
		
		boolean firstJobStatus = preprocessingJob.waitForCompletion(true);
		if(!firstJobStatus)
			throw new Exception("Preprocessing job failed.");

//		Get the total pages found based on the number of reduce input records processed and initialize variables onto the configuration.
		Long totalNodes = preprocessingJob.getCounters().findCounter(AppConstants.COUNTERS.TOTAL_NODES).getValue();
		configuration.set("TOTAL_NODES", totalNodes.toString());
		
//		End of PREPROCESSING JOB
		
		boolean secondJobStatus = false;

//		START of PAGES RANK JOB
		for(int i = 1; i <= AppConstants.MAX_RUNS; i++){
			
			Job pageRankJob = PageRankJob.configure(configuration);
			
			
			FileInputFormat.addInputPath(pageRankJob, new Path(outputPath + AppConstants.PREPROCESSING_OUTPUT + "/M-r-00000"));
			FileOutputFormat.setOutputPath(pageRankJob, new Path(outputPath + AppConstants.INTERMEDIATE_OUTPUT + i));
			
			if(i == 1) {
				pageRankJob.addCacheFile(new Path(outputPath + AppConstants.PREPROCESSING_OUTPUT + "/R-r-00000#pageRankCacheFile").toUri());
			}else {
				pageRankJob.addCacheFile(new Path(outputPath + AppConstants.INTERMEDIATE_OUTPUT + (i-1) + "#pageRankCacheFile").toUri());
			}
			
			secondJobStatus = pageRankJob.waitForCompletion(true);
			if(!secondJobStatus)
				throw new Exception("Page Rank processing job failed on the " + i + " iteration.");
			
		}
//		END of PAGE RANK JOB
		
//		START of TOP-100 JOB
		Job topKJob = TopKJob.configure(configuration);
		
		FileInputFormat.addInputPath(topKJob, new Path(outputPath + AppConstants.INTERMEDIATE_OUTPUT + AppConstants.MAX_RUNS));
		FileOutputFormat.setOutputPath(topKJob, new Path(outputPath + AppConstants.TOP100));
		
		topKJob.addCacheFile(new Path(outputPath + AppConstants.PREPROCESSING_OUTPUT + "/pageMap-r-00000#pagesMapCacheFile").toUri());
		
		boolean thirdJobStatus = topKJob.waitForCompletion(true);
//		END of TOP-100 JOB
		
		System.exit(firstJobStatus&&secondJobStatus&&thirdJobStatus?0:1);

	}

}
