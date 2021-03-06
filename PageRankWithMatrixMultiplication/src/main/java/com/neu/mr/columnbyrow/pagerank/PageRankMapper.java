/**
 * 
 */
package com.neu.mr.columnbyrow.pagerank;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
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
public class PageRankMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
	
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
			Mapper<LongWritable, Text, LongWritable, Text>.Context context)
			throws IOException, InterruptedException {
		
//		Value Format: pageNameNode~outlinkSize~outlinkNode1~outlinkNode2...
//		Ex: 0~3~1~3~4
		String[] valueParts = value.toString().split("~");
		Long rowVal = Long.parseLong(valueParts[0]);
		Long outlinkSize = Long.parseLong(valueParts[1]);
		for(int i = 2; i < valueParts.length; i++){
			Long colVal = Long.parseLong(valueParts[i]);
			Double contribution = pageRanks[rowVal.intValue()]/outlinkSize.doubleValue();
			context.write(new LongWritable(colVal), new Text(AppConstants.PAGE_RANK_CONTRIBUTION + ":" + contribution));
		}
	}

}
