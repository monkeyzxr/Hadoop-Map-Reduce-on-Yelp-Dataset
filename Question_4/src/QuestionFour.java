// Created by Xiangru Zhou

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class QuestionFour {
	private static String INPUT_PATH_1 = "";
	private static String INPUT_PATH_2 = "";
	private static String OUTPUT_PATH = "";
	
	public static class MapperClass extends Mapper<LongWritable, Text, Text,Text>{
		private Text text = new Text();
		private HashMap<String, String> hashmap = new HashMap<String, String>();
		@Override
		protected void setup(Context context) throws IOException, InterruptedException{
			Configuration conf = context.getConfiguration();
			Path[] paths = DistributedCache.getLocalCacheFiles(conf);  // put related file(business.csv) into cache
			FileReader fr = new FileReader(paths[0].toString()); //only cache one file
			BufferedReader buffreader = new BufferedReader(fr);				
			String line = null;
			while((line = buffreader.readLine()) != null){  // read the file line by line
				String[] splitArray = line.split("::"); //split the business.csv :"business_id"::"full_address"::"categories"
				hashmap.put(splitArray[0], splitArray[1]);			
			}			
		}
	
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException{
			String [] lineSplitArray = value.toString().split("::"); //split the line of review.csv: 
			                                                         //"review_id"::"user_id"::"business_id"::"stars"
			String address = hashmap.get(lineSplitArray[2]);
			if(address.contains("Stanford")){
				text.set(lineSplitArray[1]);
				context.write(text, new Text(lineSplitArray[3]));
			}		
		}		
	}
	
	public static void main(String[] args) {
		try {
			Configuration conf = new Configuration();
			String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
			if (otherArgs.length != 3) {
				System.err.println("Usage:QuestionFour <in1> <in2> <out>");
				System.exit(1);
			}
			INPUT_PATH_1 = otherArgs[0];
			INPUT_PATH_2 = otherArgs[1];
			OUTPUT_PATH = otherArgs[2];

			FileSystem fileSystem = FileSystem.get(new URI(OUTPUT_PATH), conf);
			if (fileSystem.exists(new Path(OUTPUT_PATH))) {
				fileSystem.delete(new Path(OUTPUT_PATH), true);
			}
			DistributedCache.addCacheFile(new Path(INPUT_PATH_2).toUri(), conf); //business.csv =input path_2

			Job job = new Job(conf, "QuestionFour");
			job.setJarByClass(QuestionFour.class);
			FileInputFormat.setInputPaths(job, INPUT_PATH_1);   //review.csv = input path_1
			job.setInputFormatClass(TextInputFormat.class);

			job.setMapperClass(MapperClass.class);
//			job.setReducerClass(ReduceClass.class); // No need reducer now
			
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
//			job.setOutputKeyClass(Text.class);
//			job.setOutputValueClass(Text.class);

			FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
			System.exit(job.waitForCompletion(true) ? 0 : 1);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
