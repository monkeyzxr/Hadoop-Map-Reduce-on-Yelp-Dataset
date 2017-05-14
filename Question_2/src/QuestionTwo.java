// Created by Xiangru Zhou


import java.io.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;

import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

import org.apache.hadoop.util.*;

import java.util.*;

public class QuestionTwo {
	
	// user define class
	 public static <K extends Comparable, V extends Comparable> Map<K, V> sortByValues(Map<K, V> map) {
	        List<Map.Entry<K, V>> entries = new LinkedList<Map.Entry<K, V>>(map.entrySet());

	        Collections.sort(entries, new Comparator<Map.Entry<K, V>>() {

	            @Override
	            public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
	                return o2.getValue().compareTo(o1.getValue());
	            }
	        });

	        //LinkedHashMap will keep the keys in the order they are inserted
	        //which is currently sorted on natural ordering
	        Map<K, V> sortedMap = new LinkedHashMap<K, V>();

	        for (Map.Entry<K, V> entry : entries) {
	            sortedMap.put(entry.getKey(), entry.getValue());
	        }

	        return sortedMap;
	    }	
	
	 
	// Map class
	public static class MapClass extends Mapper<LongWritable, Text, Text, FloatWritable> {		
		
		public void map(LongWritable keyIn, Text valueIn, Context context){
			try{
				String[] strWholeRecord = valueIn.toString().split("::"); // get the single line record, it's an string array
				String businessID = strWholeRecord[2];
				String rateStarString = strWholeRecord[3];
				float rateStar = Float.parseFloat(rateStarString);
				context.write(new Text(businessID), new FloatWritable(rateStar));
			}			
			catch(Exception e){
				System.out.println(e.getMessage());
			}
						
		}
	}
	
	// Reduce class
	public static class ReduceClass extends Reducer<Text, FloatWritable, Text, FloatWritable >{
		private Map<Text, FloatWritable> countMap = new HashMap<>();		
	    public void reduce(Text keyIn, Iterable<FloatWritable> valueIn, Context context) throws IOException, InterruptedException{
			//compute the aver star for a single business id
			float sum = 0;
			int count = 0;
			for(FloatWritable val : valueIn){
				sum = sum + val.get();
				count++;
			}
			float averStar = sum / count;
			//context.write(new Text(keyIn), new FloatWritable(averStar));
			
			// put the aver star of this business id into the hash map
			countMap.put(new Text(keyIn), new FloatWritable(averStar));			
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			Map<Text, FloatWritable> sortedMap = QuestionTwo.sortByValues(countMap);
			int counter = 0;
			for(Text key : sortedMap.keySet()){
				if(counter++ == 10){
					break;
				}
				context.write(key, sortedMap.get(key));			
			}
		}		
	}
	
	
	// Driver program
	public static void main(String[] args) throws Exception {
	Configuration conf = new Configuration();
	String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	// get all args
	if (otherArgs.length != 2) {                   // otherArgs means the input_dir and output_dir in the command line. Must be 2 here.
	System.err.println("Usage: QuestionTwo <in> <out>");
	System.exit(2);
	}
	// create a job with name "wordcount"
	Job job = new Job(conf, "QuestionTwo");
	job.setJarByClass(QuestionTwo.class);
	job.setMapperClass(MapClass.class);
	job.setReducerClass(ReduceClass.class);
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(FloatWritable.class);
	// uncomment the following line to add the Combiner job.setCombinerClass(Reduce.class);
	// set output key type
	job.setOutputKeyClass(Text.class);
	// set output value type
	job.setOutputValueClass(FloatWritable.class);
	//set the HDFS path of the input data
	FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	// set the HDFS path for the output
	FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	//Wait till job completion
	System.exit(job.waitForCompletion(true) ? 0 : 1);
	}		

}
