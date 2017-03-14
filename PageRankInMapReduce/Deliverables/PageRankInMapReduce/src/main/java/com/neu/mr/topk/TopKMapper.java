/**
 * 
 */
package com.neu.mr.topk;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.neu.mr.entity.PageRankEntity;
import com.neu.mr.utility.PageRankUtility;

/**
 * @author harsha
 *	
 *	Initializes a local List to hold all the page rank entity object
 *	and later emits the Top 100 pages with highest page ranks local to this mapper.
 */
public class TopKMapper extends Mapper<LongWritable, Text, NullWritable, PageRankEntity> {
	
	TreeMap<DoubleWritable, PageRankEntity> localTop100Map = new TreeMap<DoubleWritable, PageRankEntity>();
	
	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Mapper#map(java.lang.Object, java.lang.Object, org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, NullWritable, PageRankEntity>.Context context)
			throws IOException, InterruptedException {
		

//		Create a Page rank datatype from the input line.
		PageRankEntity pageRankEntity = PageRankUtility.getPageRankEntityFromValue(value, false, 0L);
		
		localTop100Map.put(pageRankEntity.getPageRank(), pageRankEntity);
		
		if(localTop100Map.size() > 100)
			localTop100Map.remove(localTop100Map.firstKey());
		
	}
	
	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Mapper#cleanup(org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	protected void cleanup(Mapper<LongWritable, Text, NullWritable, PageRankEntity>.Context context)
			throws IOException, InterruptedException {
		
		for(Entry<DoubleWritable, PageRankEntity> pageRankEntry : localTop100Map.descendingMap().entrySet()){
			context.write(NullWritable.get(), pageRankEntry.getValue());
		}
	}

	
}
