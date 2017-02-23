/**
 * 
 */
package com.neu.mr.topk;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map.Entry;

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
 */
public class TopKMapper extends Mapper<LongWritable, Text, NullWritable, PageRankEntity> {
	
	List<PageRankEntity> localTop100List = new ArrayList<PageRankEntity>();
	
	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Mapper#map(java.lang.Object, java.lang.Object, org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, NullWritable, PageRankEntity>.Context context)
			throws IOException, InterruptedException {
		

//		Create a Page rank datatype from the input line.
		PageRankEntity pageRankEntity = PageRankUtility.getPageRankEntityFromValue(value);
		
		localTop100List.add(pageRankEntity);
		
	}
	
	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Mapper#cleanup(org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	protected void cleanup(Mapper<LongWritable, Text, NullWritable, PageRankEntity>.Context context)
			throws IOException, InterruptedException {
		
		Collections.sort(localTop100List, new Comparator<PageRankEntity>() {

			@Override
			public int compare(PageRankEntity pageRankEntity1, PageRankEntity pageRankEntity2) {
				
				return pageRankEntity2.getPageRank().compareTo(pageRankEntity1.getPageRank());
			}
		});
		localTop100List = localTop100List.subList(0, 100);
		for(PageRankEntity pageRankEntity : localTop100List){
			context.write(NullWritable.get(), pageRankEntity);
		}
	}

	
}
