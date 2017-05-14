// Created by Xiangru Zhou

import java.io.*;
import java.util.HashSet;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;

import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

import org.apache.hadoop.util.*;

public class QuestionOne {
	
	// Map class
	public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable keyIn, Text valueIn, Context context){
			try{
				String[] str = valueIn.toString().split("::");
				String address = str[1];
				String targetAddress = "Palo Alto";
				if(address.contains(targetAddress)){
					context.write(new Text(address), new Text(valueIn));
				}
			}			
			catch(Exception e){
				System.out.println(e.getMessage());
			}
						
		}
	}

	// Reduce Class
		public static class ReduceClass extends Reducer<Text, Text, Text, Text> {
			HashSet<String> hashset = new HashSet<String>();
			public void reduce(Text keyIn, Iterable<Text> valueIn, Context context) throws IOException, InterruptedException{							
				for(Text val : valueIn){ //for a single valueIn whole record
					String [] strArrayWholeRecord = val.toString().split("::");  // split a single whole long record with ::, get a string array
					String strCategory = strArrayWholeRecord[2].substring(5, strArrayWholeRecord[2].length()-1); //get the string of category, delete the ( and )
					                                                                                             //substring parameter: end_index is not contained
					String[] strSingleShortCategory = strCategory.split(", ");   // get the single short category string					
					
					for(int i = 0; i < strSingleShortCategory.length; i++){    // array.length: how many elements in the array
						if(strSingleShortCategory[i].equals("")){              // space not shown
							continue;
						}
						boolean suc = hashset.add(strSingleShortCategory[i]);
						if(suc){
							context.write(new Text(strSingleShortCategory[i]), new Text(""));   // make category result as key, value not shown
						}						
					}			
				}			
			}
		}
	
	
	
	// Driver program
		public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		// get all args
		if (otherArgs.length != 2) {                   // otherArgs means the input_dir and output_dir in the command line. Must be 2 here.
		System.err.println("Usage: QuestionOne <in> <out>");
		System.exit(2);
		}
		// create a job with name "wordcount"
		Job job = new Job(conf, "QuestionOne");
		job.setJarByClass(QuestionOne.class);
		job.setMapperClass(MapClass.class);
		job.setReducerClass(ReduceClass.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		// uncomment the following line to add the Combiner job.setCombinerClass(Reduce.class);
		// set output key type
		job.setOutputKeyClass(Text.class);
		// set output value type
		job.setOutputValueClass(Text.class);
		//set the HDFS path of the input data
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		// set the HDFS path for the output
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		//Wait till job completion
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		}	
	

	

}
