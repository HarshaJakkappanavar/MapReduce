/**
 * 
 */
package com.neu.mr.utility;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import com.neu.mr.constants.AppConstants;
import com.neu.mr.entity.PageRankEntity;

/**
 * @author harsha
 *
 */
public class PageRankUtility {
	

	/**
	 * @param value
	 * @return PageRankEntity object
	 * 
	 * 	Parses the input line, extract the attributes and returns a PageRankEntity object
	 */
	public static PageRankEntity getPageRankEntityFromValue(Text value) {

		StringBuilder stringBuilder = new StringBuilder(value.toString());
		Double pageRank = null;
		
		int indexOfTilde = stringBuilder.indexOf("~");
		String pageName = stringBuilder.substring(0, indexOfTilde);
		stringBuilder.delete(0, ++indexOfTilde);
		
		indexOfTilde = stringBuilder.indexOf("~");
//		if(AppConstants.isFirstIteration){
		if(AppConstants.isFirstIteration){
			pageRank = 1/AppConstants.totalPages.doubleValue();
		}else {
			String pageRankText = stringBuilder.substring(0, indexOfTilde);
			pageRank = Double.valueOf(pageRankText);
		}
		stringBuilder.delete(0, ++indexOfTilde);
		
		indexOfTilde = stringBuilder.indexOf("~");
		String outlinkSizeText = stringBuilder.substring(0, indexOfTilde);
		int outlinkSize = Integer.valueOf(outlinkSizeText);
		stringBuilder.delete(0, ++indexOfTilde);
		
		indexOfTilde = stringBuilder.indexOf("~");
		List<Text> outlinks = new ArrayList<Text>();
		while(indexOfTilde != -1){
			
			String outlink = stringBuilder.substring(0, indexOfTilde);
			outlinks.add(new Text(outlink));
			stringBuilder.delete(0, ++indexOfTilde);
			indexOfTilde = stringBuilder.indexOf("~");
			
		}

		return new PageRankEntity(new Text(pageName), 
				new DoubleWritable(pageRank), 
				new IntWritable(outlinkSize),
				new ArrayList<Text>(outlinks));
	}

}
