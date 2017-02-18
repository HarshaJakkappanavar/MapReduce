/**
 * 
 */
package com.neu.mr.combiner;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
public class Combiner {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		
		Configuration configuration = new Configuration();
		
		String[] cmdLineArgs = new GenericOptionsParser(configuration, args).getRemainingArgs();
		if(cmdLineArgs.length < 2){
			System.err.println("Usage: hadoop jar <NameofJar.jar> <JobName> <InputDirectory> <OutputDirectory>");
			System.exit(2);
		}
		
		Job job = new Job(configuration, "Combiner");
		job.setJarByClass(Combiner.class);
		job.setMapperClass(CombinerMapper.class);
		job.setReducerClass(CombinerReducer.class);
		job.setCombinerClass(CombinerClass.class);
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

/**
 * @author harsha
 *
 */
class CombinerMapper extends Mapper<LongWritable, Text, Text, TemperatureAccumulator> {

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
//			Populates the accumulator datastructure with temprature type and temperature
			TemperatureAccumulator temperatureAccumulator = new TemperatureAccumulator(tempTypeFromLine, temperature);
			context.write(new Text(stationId), temperatureAccumulator);
		}
		
		
	}
	
	
}

/**
 * @author harsha
 *
 */
class CombinerReducer extends Reducer<Text, TemperatureAccumulator, Text, MeanTemperatureOutput> {

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
//				updates the count and temperature to the running TMIN temperature accumulator
				meanTemperatureOutput.updateTMinMeanAccumulator(temperatureAccumulator.getTemperature(), temperatureAccumulator.getCountSoFar().get());
			}else if(temperatureAccumulator.getTemperatureType() == AppConstants.TMAX_VALUE){
//				updates the count and temperature to the running TMAX temperature accumulator
				meanTemperatureOutput.updateTMaxMeanAccumulator(temperatureAccumulator.getTemperature(), temperatureAccumulator.getCountSoFar().get());
			}
		}
		
		context.write(key, meanTemperatureOutput);
	}
	
	
}

/**
 * @author harsha
 *
 */
class CombinerClass extends Reducer<Text, TemperatureAccumulator, Text, TemperatureAccumulator> {

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
	 */
	@Override
	protected void reduce(Text key, Iterable<TemperatureAccumulator> temperatureAccumulators,
			Reducer<Text, TemperatureAccumulator, Text, TemperatureAccumulator>.Context context)
			throws IOException, InterruptedException {
//		We create two temperature accumulators one for TMIN and one for TMAX
		TemperatureAccumulator tMinTemperatureAccumulator = new TemperatureAccumulator();
		tMinTemperatureAccumulator.setTemperatureType(AppConstants.TMIN_VALUE);
		TemperatureAccumulator tMaxTemperatureAccumulator = new TemperatureAccumulator();
		tMaxTemperatureAccumulator.setTemperatureType(AppConstants.TMAX_VALUE);
		
		for(TemperatureAccumulator temperatureAccumulator : temperatureAccumulators){
			if(temperatureAccumulator.getTemperatureType() == AppConstants.TMIN_VALUE){
//				updates the count and temperature to the running TMIN temperature accumulator
				tMinTemperatureAccumulator.updateTemperature(temperatureAccumulator.getTemperature());
				tMinTemperatureAccumulator.updateCountSoFar(temperatureAccumulator.getCountSoFar());
			}else if(temperatureAccumulator.getTemperatureType() == AppConstants.TMAX_VALUE){
//				updates the count and temperature to the running TMAX temperature accumulator
				tMaxTemperatureAccumulator.updateTemperature(temperatureAccumulator.getTemperature());
				tMaxTemperatureAccumulator.updateCountSoFar(temperatureAccumulator.getCountSoFar());
			}
		}
		
		context.write(key, tMinTemperatureAccumulator);
		context.write(key, tMaxTemperatureAccumulator);
		
	}
	
	
}






