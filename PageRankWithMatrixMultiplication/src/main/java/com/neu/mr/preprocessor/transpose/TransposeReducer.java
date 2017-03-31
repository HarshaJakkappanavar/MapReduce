/**
 * 
 */
package com.neu.mr.preprocessor.transpose;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author harsha
 *
 *	Picks the first object in the Iterable "value" and emits it.
 */
public class TransposeReducer extends Reducer<LongWritable, Text, NullWritable, Text> {
	
	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
	 */
	@Override
	protected void reduce(LongWritable key, Iterable<Text> value, Reducer<LongWritable, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		
		StringBuilder stringBuilder = new StringBuilder();
		stringBuilder.append(key + "~");
		
		for(Iterator<Text> valueIterator = value.iterator(); valueIterator.hasNext();){
			String valueString = valueIterator.next().toString();
			if(!valueString.isEmpty()){
				stringBuilder.append(valueString);
				if(valueIterator.hasNext()){
					stringBuilder.append("~");
				}
			}
		}
		context.write(NullWritable.get(), new Text(stringBuilder.toString()));
	}
}
