/**
 * 
 */
package com.neu.mr.columnbyrow.pagerank;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.neu.mr.constants.AppConstants; 

/**
 * @author harsha
 *
 */
public class DanglingNodeMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
	
	private static Long TOTAL_NODES;
	private static Double[] pageRanks;
	
	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	protected void setup(Mapper<LongWritable, Text, LongWritable, Text>.Context context)
			throws IOException, InterruptedException {

		Configuration configuration = context.getConfiguration();
		
		TOTAL_NODES = configuration.getLong("TOTAL_NODES", 0L);
		pageRanks = new Double[TOTAL_NODES.intValue()];
		
		File pageRankCacheFile = new File("./pageRankCacheFile");
		BufferedReader bufferedReader = new BufferedReader(new FileReader(pageRankCacheFile));
		String line;
		while((line = bufferedReader.readLine()) != null){
			String[] lineParts = line.split(":");
			int index = Integer.parseInt(lineParts[0]);
			double pageRank = Double.parseDouble(lineParts[1]);
			pageRanks[index] = pageRank;
		}
		bufferedReader.close();
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Mapper#map(java.lang.Object, java.lang.Object, org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, LongWritable, Text>.Context context)
			throws IOException, InterruptedException {
		
		Long rowVal = Long.parseLong(value.toString());
		for(int colVal = 0; colVal < TOTAL_NODES; colVal++){
			Double contribution = pageRanks[rowVal.intValue()]/TOTAL_NODES.doubleValue();
			context.write(new LongWritable(colVal), new Text(AppConstants.DANGLING_NODE_CONTRIBUTION + ":" + contribution));
		}
		
	}

}
