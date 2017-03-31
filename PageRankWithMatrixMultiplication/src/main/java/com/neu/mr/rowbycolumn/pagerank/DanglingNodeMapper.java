/**
 * 
 */
package com.neu.mr.rowbycolumn.pagerank;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.neu.mr.constants.AppConstants; 

/**
 * @author harsha
 *
 */
public class DanglingNodeMapper extends Mapper<LongWritable, Text, NullWritable, NullWritable> {
	
	private static Long TOTAL_NODES;
	private static Double[] pageRanks;
	private static Double danglingContribution = 0.0;
	
	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	protected void setup(Mapper<LongWritable, Text, NullWritable, NullWritable>.Context context)
			throws IOException, InterruptedException {

		Configuration configuration = context.getConfiguration();
		
		danglingContribution = 0.0;
		TOTAL_NODES = configuration.getLong("TOTAL_NODES", 0L);
		pageRanks = new Double[TOTAL_NODES.intValue()];
		
		String pageRankCacheFileName = configuration.get("PAGE_RANK_CACHE_FILE");
		File pageRankCacheFile = new File("./"+pageRankCacheFileName);
		if(pageRankCacheFile.isDirectory()){
			File[] pageRankCacheFiles = pageRankCacheFile.listFiles();
			for(File file : pageRankCacheFiles){
				if(!file.getName().contains(".crc")
						&& !file.getName().contains("crc")){
					populatePageRanks(file);
				}
			}
		}else{
			populatePageRanks(pageRankCacheFile);
		}
		
	}

	private void populatePageRanks(File pageRankCacheFile) throws NumberFormatException, IOException {
		BufferedReader bufferedReader = new BufferedReader(new FileReader(pageRankCacheFile));
		String line;
		while((line = bufferedReader.readLine()) != null){
			String[] lineParts = line.split(":");
			int index = Integer.parseInt(lineParts[0]);
			double pageRank = Double.valueOf(lineParts[1]);
			pageRanks[index] = pageRank;
		}
		bufferedReader.close();
		
	}
	
	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Mapper#map(java.lang.Object, java.lang.Object, org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, NullWritable, NullWritable>.Context context)
			throws IOException, InterruptedException {
		
		Long indexVal = Long.parseLong(value.toString());
		danglingContribution += pageRanks[indexVal.intValue()];
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Mapper#cleanup(org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	protected void cleanup(Mapper<LongWritable, Text, NullWritable, NullWritable>.Context context)
			throws IOException, InterruptedException {
		context.getCounter(AppConstants.COUNTERS.DANGLING_FACTOR).setValue(Double.doubleToLongBits(danglingContribution/TOTAL_NODES.doubleValue()));
	}

	
}
