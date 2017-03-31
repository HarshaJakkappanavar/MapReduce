/**
 * 
 */
package com.neu.mr.preprocessor.transpose;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author harsha
 *
 *	Uses the parser utility provided and parses the .bz2 formatted file to a simple application(this application) friendly file (more focused on what's important). 
 */
public class TransposeMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
	
	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	protected void setup(Mapper<LongWritable, Text, LongWritable, Text>.Context context)
			throws IOException, InterruptedException {

		Configuration configuration = context.getConfiguration();
		
		Long TOTAL_NODES = configuration.getLong("TOTAL_NODES", 0L);
		for(int i = 0; i < TOTAL_NODES; i++){
			context.write(new LongWritable(i), new Text(""));
		}
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
		
		context.write(new LongWritable(rowVal), new Text(""));
		
		for(int i = 2; i < valueParts.length; i++){
			Long colVal = Long.parseLong(valueParts[i]);
			context.write(new LongWritable(colVal), new Text(rowVal + ":" + outlinkSize));
		}
		
	}

}
