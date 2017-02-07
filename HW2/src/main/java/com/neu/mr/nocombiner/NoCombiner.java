/**
 * 
 */
package com.neu.mr.nocombiner;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.neu.mr.constants.AppConstants;
import com.neu.mr.entity.MeanTemperatureOutput;
import com.neu.mr.entity.TemperatureAccumulator;

/**
 * @author harsha
 *
 */
public class NoCombiner {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		
//		
		// TODO Auto-generated method stub
		
		Configuration configuration = new Configuration();
		
		String[] cmdLineArgs = new GenericOptionsParser(configuration, args).getRemainingArgs();
		if(cmdLineArgs.length < 2){
			System.err.println("Usage: hadoop jar <NameofJar.jar> <JobName> <InputDirectory> <OutputDirectory>");
			System.exit(2);
		}
		
		Job job = new Job(configuration, "NoCombiner");
		job.setJarByClass(NoCombiner.class);
		job.setMapperClass(NoCombinerMapper.class);
		job.setReducerClass(NoCombinerReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(TemperatureAccumulator.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(MeanTemperatureOutput.class);
		
		for(int i = 0; i < cmdLineArgs.length - 1; i++){
			FileInputFormat.addInputPath(job, new Path(cmdLineArgs[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(cmdLineArgs[cmdLineArgs.length - 1]));
		System.exit(job.waitForCompletion(true)?0:1);
		

	}

}

class NoCombinerMapper extends Mapper<LongWritable, Text, Text, TemperatureAccumulator> {

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Mapper#map(java.lang.Object, java.lang.Object, org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, TemperatureAccumulator>.Context context)
			throws IOException, InterruptedException {
		
		String line = value.toString();
		String[] lineArray = line.split(",");
		
		String tempTypeFromLine = lineArray[2].trim();
		
		if(tempTypeFromLine.equalsIgnoreCase(AppConstants.TMAX_TEXT) 
				|| tempTypeFromLine.equalsIgnoreCase(AppConstants.TMIN_TEXT)){
			
			int temperature = Integer.parseInt(lineArray[3]);
			String stationId = lineArray[0];
			
			TemperatureAccumulator temperatureAccumulator = new TemperatureAccumulator(tempTypeFromLine, temperature);
			context.write(new Text(stationId), temperatureAccumulator);
		}
		
		
	}
	
	
}

class NoCombinerReducer 
extends Reducer<Text, TemperatureAccumulator, Text, MeanTemperatureOutput> {

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
	 */
	@Override
	protected void reduce(Text key, Iterable<TemperatureAccumulator> temperatureAccumulators,
			Reducer<Text, TemperatureAccumulator, Text, MeanTemperatureOutput>.Context context)
			throws IOException, InterruptedException {

		MeanTemperatureOutput meanTemperatureOutput = new MeanTemperatureOutput();
		
		for(TemperatureAccumulator temperatureAccumulator : temperatureAccumulators){
			if(temperatureAccumulator.getTemperatureType() == AppConstants.TMIN_VALUE){
				meanTemperatureOutput.updateTMinMeanAccumulator(temperatureAccumulator.getTemperature(), 1);
			}else if(temperatureAccumulator.getTemperatureType() == AppConstants.TMAX_VALUE){
				meanTemperatureOutput.updateTMaxMeanAccumulator(temperatureAccumulator.getTemperature(), 1);
			}
		}
		
		context.write(key, meanTemperatureOutput);
	}
	
	
}








