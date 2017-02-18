/**
 * 
 */
package com.neu.mr.inmappercomb;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

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
public class InMapperComb {

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
		
		Job job = new Job(configuration, "InMapperComb");
		job.setJarByClass(InMapperComb.class);
		job.setMapperClass(InMapperCombMapper.class);
		job.setReducerClass(InMapperCombReducer.class);
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
class InMapperCombMapper extends Mapper<LongWritable, Text, Text, TemperatureAccumulator> {

//	Declare the local aggregation datastructure. 
//	the temperature accumulator is mapped by station id, by temperature type.
	Map<String, Map<Integer, TemperatureAccumulator>> temperatureAccumulatorMap;
	

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	protected void setup(Mapper<LongWritable, Text, Text, TemperatureAccumulator>.Context context)
			throws IOException, InterruptedException {
		temperatureAccumulatorMap = new HashMap<String, Map<Integer, TemperatureAccumulator>>();
	}
	
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
			
//			Initializing the three attributes of the accumulator data structure.
			int temperature = Integer.parseInt(lineArray[3]);
			Integer temperatureType = tempTypeFromLine.equalsIgnoreCase(AppConstants.TMAX_TEXT)?AppConstants.TMAX_VALUE:AppConstants.TMIN_VALUE;
			String stationId = lineArray[0].trim();
			
//			get the map (with temperature type as key and temperature accumulator as value) by station id, initialize if null. 
			Map<Integer, TemperatureAccumulator> temperatureTypeAccumulator = temperatureAccumulatorMap.get(stationId);
			if(null == temperatureTypeAccumulator){
				temperatureTypeAccumulator = new HashMap<Integer, TemperatureAccumulator>();
			}
//			get the temperature accumulator by temperature type, initialize if null. Also update the temperature and count.
			TemperatureAccumulator temperatureAccumulator = temperatureTypeAccumulator.get(temperatureType);
			if(null == temperatureAccumulator){
				temperatureAccumulator = new TemperatureAccumulator(tempTypeFromLine, temperature);
			}else{
				temperatureAccumulator.updateTemperature(temperature);
				temperatureAccumulator.updateCountSoFar(new IntWritable(1));
			}
//			put the values back to the local aggregation data structure.
			temperatureTypeAccumulator.put(temperatureType, temperatureAccumulator);
			temperatureAccumulatorMap.put(stationId, temperatureTypeAccumulator);
		}
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Mapper#cleanup(org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	protected void cleanup(Mapper<LongWritable, Text, Text, TemperatureAccumulator>.Context context)
			throws IOException, InterruptedException {
//		Emits the output
		for(Entry<String, Map<Integer, TemperatureAccumulator>> tempAccumulatorMapEntry : temperatureAccumulatorMap.entrySet()){
			for(Entry<Integer, TemperatureAccumulator> tempTypeAccumulatorEntry : tempAccumulatorMapEntry.getValue().entrySet()){
				context.write(new Text(tempAccumulatorMapEntry.getKey()), tempTypeAccumulatorEntry.getValue());
			}
		}
	}
	
}

/**
 * @author harsha
 *
 */
class InMapperCombReducer extends Reducer<Text, TemperatureAccumulator, Text, MeanTemperatureOutput> {

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



