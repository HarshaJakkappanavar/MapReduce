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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.neu.mr.entity.PageRankEntity;

/**
 * @author harsha
 *
 */
public class TopKReducer extends Reducer<NullWritable, PageRankEntity, NullWritable, Text> {

	List<PageRankEntity> globalTop100Map = new ArrayList<PageRankEntity>();
	
	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
	 */
	@Override
	protected void reduce(NullWritable key, Iterable<PageRankEntity> value,
			Reducer<NullWritable, PageRankEntity, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {

		for(PageRankEntity pageRankEntity : value){
			PageRankEntity listElement = new PageRankEntity(new Text(pageRankEntity.getPageName()), 
					new DoubleWritable(pageRankEntity.getPageRank().get()), 
					new IntWritable(pageRankEntity.getOutlinkSize()),
					pageRankEntity.getOutlinks());
		
			globalTop100Map.add(listElement);
			
		}

		
		Collections.sort(globalTop100Map, new Comparator<PageRankEntity>() {

			@Override
			public int compare(PageRankEntity pageRankEntity1, PageRankEntity pageRankEntity2) {
				
				return pageRankEntity2.getPageRank().compareTo(pageRankEntity1.getPageRank());
			}
		});
		globalTop100Map = globalTop100Map.subList(0, 100);
		for(PageRankEntity pageRankEntity : globalTop100Map){
			context.write(NullWritable.get(), new Text(pageRankEntity.toString()));
		}
	
	}
	
	
}
