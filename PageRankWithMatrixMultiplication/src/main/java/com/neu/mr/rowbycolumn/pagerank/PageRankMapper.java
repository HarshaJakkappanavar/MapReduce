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
public class PageRankMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
	
	private static Long TOTAL_NODES;
	private static Double[] pageRanks;
	private static Double danglingContribution;
	
	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	protected void setup(Mapper<LongWritable, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {

		Configuration configuration = context.getConfiguration();
		
		TOTAL_NODES = configuration.getLong("TOTAL_NODES", 0L);
		Long danglingFactorLong = configuration.getLong("DANGLING_FACTOR", 0L);
		danglingContribution = Double.longBitsToDouble(danglingFactorLong);
		
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
			Mapper<LongWritable, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		
//		Value Format: pageNameNode~incomingNode1:outlinkSizeOf1~incomingNode2:outlinkSizeOf2~...
//		Ex: 0~1:3~2:1~5:3
		String[] valueParts = value.toString().split("~");
		Long rowVal = Long.parseLong(valueParts[0]);
		Double incomingContribution = 0.0;
		for(int i = 1; i < valueParts.length; i++){
			String[] incomingNodeParts = valueParts[i].split(":");
			Long incomingNode = Long.valueOf(incomingNodeParts[0]);
			Long outlinkSize = Long.valueOf(incomingNodeParts[1]);
			incomingContribution += pageRanks[incomingNode.intValue()]/outlinkSize.doubleValue();
		}
		
		Double newPageRank = (AppConstants.ALPHA_VALUE / TOTAL_NODES.doubleValue()) + (AppConstants.INVERSE_ALPHA_VALUE * (incomingContribution + danglingContribution));
		context.write(NullWritable.get(), new Text(rowVal + ":" + newPageRank));
		
	}

}
