/**
 * 
 */
package com.neu.mr.secondarysort;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
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
import com.neu.mr.secondarysort.entity.GroupComparator;
import com.neu.mr.secondarysort.entity.HashPartitioner;
import com.neu.mr.secondarysort.entity.KeyComparator;
import com.neu.mr.secondarysort.entity.StationYearKey;

/**
 * @author harsha
 *
 */
public class SecondarySort {

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
		
		Job job = new Job(configuration, "NoCombiner");
		job.setJarByClass(SecondarySort.class);
		
		job.setMapperClass(SecondarySortMapper.class);
		job.setMapOutputKeyClass(StationYearKey.class);
		job.setMapOutputValueClass(TemperatureAccumulator.class);

		job.setReducerClass(SecondarySortReducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		job.setPartitionerClass(HashPartitioner.class);
		job.setSortComparatorClass(KeyComparator.class);
		job.setGroupingComparatorClass(GroupComparator.class);
		
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
class SecondarySortMapper extends Mapper<LongWritable, Text, StationYearKey, TemperatureAccumulator> {

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Mapper#map(java.lang.Object, java.lang.Object, org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, StationYearKey, TemperatureAccumulator>.Context context)
			throws IOException, InterruptedException {
		
		String line = value.toString();
		String[] lineArray = line.split(",");
		
		String tempTypeFromLine = lineArray[2].trim();
		
		if(tempTypeFromLine.equalsIgnoreCase(AppConstants.TMAX_TEXT) 
				|| tempTypeFromLine.equalsIgnoreCase(AppConstants.TMIN_TEXT)){
			
			String stationId = lineArray[0];
			String yearInText = lineArray[1];
			int year = Integer.parseInt(yearInText.substring(0, 4));
			int temperature = Integer.parseInt(lineArray[3]);
			
			StationYearKey stationYearKey = new StationYearKey(stationId, year);
//			Populates the accumulator datastructure with temprature type and temperature
			TemperatureAccumulator temperatureAccumulator = new TemperatureAccumulator(tempTypeFromLine, temperature);

			context.write(stationYearKey, temperatureAccumulator);
		}
		
		
	}
	
	
}

/**
 * @author harsha
 *
 */
class SecondarySortReducer extends Reducer<StationYearKey, TemperatureAccumulator, NullWritable, Text> {
	
	NullWritable nw =  NullWritable.get();

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
	 */
	@Override
	protected void reduce(StationYearKey key, Iterable<TemperatureAccumulator> temperatureAccumulators,
			Reducer<StationYearKey, TemperatureAccumulator, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		
		String stationId = key.getStationId().toString();
//		Hold the changing year in a station.
		int year = key.getYear().get();
//		Holds the output mean temperature both min and max.
		MeanTemperatureOutput meanTemperatureOutput = new MeanTemperatureOutput();
//		Constructs the output string.
		StringBuilder stringBuilder = new StringBuilder();
		stringBuilder.append(stationId + ", [(");
		
		for(TemperatureAccumulator temperatureAccumulator : temperatureAccumulators){
//			update the output string everytime a new year is encountered. Re-initialize the variables.
			if(key.getYear().get() != year){
				stringBuilder.append(year + ", " + meanTemperatureOutput.toString() + "), (");
				year = key.getYear().get();
				meanTemperatureOutput = new MeanTemperatureOutput();
			}
//			Builds up the sum of temperatures and counts for both TMIN and TMAX
			if(temperatureAccumulator.getTemperatureType() == AppConstants.TMIN_VALUE){
				meanTemperatureOutput.updateTMinMeanAccumulator(temperatureAccumulator.getTemperature(), 1);
			}else if(temperatureAccumulator.getTemperatureType() == AppConstants.TMAX_VALUE){
				meanTemperatureOutput.updateTMaxMeanAccumulator(temperatureAccumulator.getTemperature(), 1);
			}
		}
		
//		constructs the output of the last year for this station and writes it on the context.
		stringBuilder.append(year + ", " + meanTemperatureOutput.toString() + ")]");
		context.write(nw, new Text(stringBuilder.toString()));
	}
	
	
}








