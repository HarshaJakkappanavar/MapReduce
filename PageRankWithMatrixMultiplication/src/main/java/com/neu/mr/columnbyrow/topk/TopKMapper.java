/**
 * 
 */
package com.neu.mr.columnbyrow.topk;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author harsha
 *	
 *	Initializes a local TreeMap to hold all the pages rank entries from the input file.
 *	and later emits the Top 100 pages with highest page ranks local to this mapper.
 */
public class TopKMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
	
	private static Long TOTAL_NODES;
	private static String[] pagesMap;
	TreeMap<DoubleWritable, LongWritable> localTop100Map = new TreeMap<DoubleWritable, LongWritable>();
	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	protected void setup(Mapper<LongWritable, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {

		Configuration configuration = context.getConfiguration();
		
		TOTAL_NODES = configuration.getLong("TOTAL_NODES", 0L);
		pagesMap = new String[TOTAL_NODES.intValue()];
		
		File pagesMapCacheFile = new File("./pagesMapCacheFile");
		BufferedReader bufferedReader = new BufferedReader(new FileReader(pagesMapCacheFile));
		String line;
		while((line = bufferedReader.readLine()) != null){
			String[] lineParts = line.split("\t");
			String pageName = lineParts[0];
			int mappedIndex = Integer.parseInt(lineParts[1]);
			
			pagesMap[mappedIndex] = pageName;
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
		

//		Extract the Page rank from the input line.
		String[] valueParts = value.toString().split(":");
		double pageRank = Double.parseDouble(valueParts[1]);
		long mappedPage = Long.parseLong(valueParts[0]); 
		
		localTop100Map.put(new DoubleWritable(pageRank), new LongWritable(mappedPage));
		
		if(localTop100Map.size() > 100)
			localTop100Map.remove(localTop100Map.firstKey());
		
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Mapper#cleanup(org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	protected void cleanup(Mapper<LongWritable, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		
		for(Entry<DoubleWritable, LongWritable> pageRankEntry : localTop100Map.descendingMap().entrySet()){
			String pageName = pagesMap[(int) pageRankEntry.getValue().get()];
			context.write(NullWritable.get(), new Text(pageName + "=" + pageRankEntry.getKey().get()));
		}
	}

	
}
