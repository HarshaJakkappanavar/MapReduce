/**
 * 
 */
package com.neu.mr.topk;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.TreeMap;
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
 *
 *	Receives the local top 100 pages from all the mappers into a single reducer.
 *	emits the top 100 pages from this list based on the higher page rank values,
 *	making these pages globally among the top 100 pages with better page rank values.
 */
public class TopKReducer extends Reducer<NullWritable, PageRankEntity, NullWritable, Text> {

	TreeMap<DoubleWritable, PageRankEntity> globalTop100Map = new TreeMap<DoubleWritable, PageRankEntity>();
	
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
		
			globalTop100Map.put(listElement.getPageRank(), listElement);
			
			if(globalTop100Map.size() > 100)
				globalTop100Map.remove(globalTop100Map.firstKey());
			
			
		}

//		Sort in the descending order of the page rank values.
		for(Entry<DoubleWritable, PageRankEntity> pageRankEntry : globalTop100Map.descendingMap().entrySet()){
			context.write(NullWritable.get(), new Text(pageRankEntry.getValue().toStringForTopK()));
		}
		
	}
	
	
}
