/**
 * 
 */
package com.neu.mr.preprocessor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.neu.mr.constants.AppConstants;
import com.neu.mr.entity.PageRankEntity;

/**
 * @author harsha
 *
 *	Picks the first object in the Iterable "value" and emits it.
 */
public class PreProcessingReducer extends Reducer<PageRankEntity, PageRankEntity, Text, Text> {
	
	public static Map<String, Long> pagesMapping;
	public static Set<Long> danglingNodes;
	public static Long count = 0L;
	public static MultipleOutputs<Text, Text> multipleOutputs;

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
	 */
	@Override
	protected void setup(Reducer<PageRankEntity, PageRankEntity, Text, Text>.Context context)
			throws IOException, InterruptedException {
		pagesMapping = new HashMap<String, Long>();
		danglingNodes = new HashSet<Long>();
		multipleOutputs = new MultipleOutputs<Text, Text>(context);
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
	 */
	@Override
	protected void reduce(PageRankEntity key, Iterable<PageRankEntity> value, Reducer<PageRankEntity, PageRankEntity, Text, Text>.Context context)
			throws IOException, InterruptedException {
		
		PageRankEntity pageRankEntity = null;
		
		for(PageRankEntity valueEntity : value){
			pageRankEntity = valueEntity;
			break;
		}
	
		String pageName = pageRankEntity.getPageName();
		int outlinkSize = pageRankEntity.getOutlinkSize();
		
		if(!pagesMapping.containsKey(pageName)){
			pagesMapping.put(pageName, count);
			multipleOutputs.write("pageMap", new Text(pageName), new Text(String.valueOf(count++)));
		}
		
		List<Long> outlinksMap = new ArrayList<Long>();
		for(Text outlink : pageRankEntity.getOutlinks()){
			Long pageMap = pagesMapping.get(outlink.toString());
			if(null == pageMap){
				pageMap = count++;
				pagesMapping.put(outlink.toString(), pageMap);
				multipleOutputs.write("pageMap", outlink, new Text(String.valueOf(pageMap)));
			}
			outlinksMap.add(pagesMapping.get(outlink.toString()));
		}
		
		if(outlinksMap.isEmpty()){
			danglingNodes.add(pagesMapping.get(pageName));
		}else {
			StringBuilder stringBuilder = new StringBuilder();
			stringBuilder.append(pagesMapping.get(pageName) + "~" + outlinkSize);
			for(Iterator<Long> mapIterator = outlinksMap.iterator(); mapIterator.hasNext();){
				stringBuilder.append("~" + mapIterator.next());
			}
			multipleOutputs.write("M", NullWritable.get(), new Text(stringBuilder.toString()));
		}
		
	}
	
	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Reducer#cleanup(org.apache.hadoop.mapreduce.Reducer.Context)
	 */
	@Override
	protected void cleanup(Reducer<PageRankEntity, PageRankEntity, Text, Text>.Context context)
			throws IOException, InterruptedException {
		final Long TOTAL_NODES = count;
		final Double DEFAULT_VALUE = 1/TOTAL_NODES.doubleValue();
		for(Long danglingNode : danglingNodes){
			multipleOutputs.write("D", NullWritable.get(), new Text(danglingNode.toString()));
		}
//		StringBuilder stringBuilder = new StringBuilder();
		for(long i = 0L; i < TOTAL_NODES; i++){
			multipleOutputs.write("R", NullWritable.get(), new Text(i + ":" + DEFAULT_VALUE));
//			stringBuilder.append("~" + i);
		}
//		for(Long danglingNode : danglingNodes){
//			multipleOutputs.write("M", NullWritable.get(), new Text(danglingNode + "~" + TOTAL_NODES + stringBuilder.toString()));
//		}
		multipleOutputs.close();
		context.getCounter(AppConstants.COUNTERS.TOTAL_NODES).setValue(TOTAL_NODES);
	}
}
