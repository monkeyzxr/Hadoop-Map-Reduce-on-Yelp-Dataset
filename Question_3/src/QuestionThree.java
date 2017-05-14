// Created by Xiangru Zhou


import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class QuestionThree {
	/*Job1: get the top 10 of business_id and avg_rating*/
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
		public static class MapClassJobOne extends Mapper<LongWritable, Text, Text, FloatWritable> {		
			
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
		public static class ReduceClassJobOne extends Reducer<Text, FloatWritable, Text, Text >{//FloatWritable
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
				Map<Text, FloatWritable> sortedMap = QuestionThree.sortByValues(countMap);
				int counter = 0;
				String valueToString = "";
				for(Text key : sortedMap.keySet()){
					if(counter++ == 10){
						break;
					}
					valueToString = String.valueOf(sortedMap.get(key));
					//context.write(key, new Text(sortedMap.get(key).toString()));	
					context.write(key, new Text(valueToString));
				}
			}		
		}

	/*finished the job 1*/

		
		
		
	/* Job 2: reduce-side join, to get:    business_id   full_address   categories  avg_rating */              
	
	// Map class for business.csv -- "business_id"::"full_address"::"categories"
	public static class BusinessMapper extends Mapper<LongWritable, Text, Text, Text> {
		private Text outKey = new Text();
		private Text outValue = new Text();	
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			String line = value.toString();
			String[] items = line.split("::");//or tab
			outKey.set(items[0]);
			outValue.set("A" + value.toString());
			context.write(outKey, outValue);			
		}
	}
	
	// Map class for Job1 of review.csv -- "business_id" "stars"
	public static class ReviewMapper extends Mapper<LongWritable, Text, Text, Text> {// input type change here
		private Text outKey = new Text();
		private Text outValue = new Text();
	//	private FloatWritable outValue = new FloatWritable();
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			String line = value.toString();
			String[] items = line.split("\t"); // split by space error
			outKey.set(items[0]);     
			outValue.set("B" + items[1]);
			context.write(outKey, outValue);	
		}
	}
	
	// reduce-side join ;;use inner join here
	public static class JoinReducer extends Reducer<Text, Text, Text, Text>{
      //  private List<Text> listA = new ArrayList<Text>();   //listA contains business
		private HashSet<Text> hashSetA = new HashSet<Text>();
        //private List<Text> listB = new ArrayList<Text>();    // listB contains top 10 review
		private HashSet<Text> hashSetB = new HashSet<Text>();
		
        @Override  
        protected void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {  
           // listA.clear();  
            //listB.clear(); 
        	hashSetA.clear();
        	hashSetB.clear();
        	
            Iterator<Text> iterator = values.iterator();  
            while(iterator.hasNext()){  
                String value = iterator.next().toString();  
                if(value.charAt(0)=='A')  
                   // listA.add(new Text(value.substring(1)));  
                	hashSetA.add(new Text(value.substring(1)));
                if(value.charAt(0)=='B')  
                   // listB.add(new Text(value.substring(1))); 
                	hashSetB.add(new Text(value.substring(1)));
            }  
            //joinAndWrite(context);  // inner join
            if(!hashSetA.isEmpty() && !hashSetB.isEmpty()) {  
                for (Text A : hashSetA)  
                    for(Text B : hashSetB){  
                        context.write(A, B);  
                    }  
            }  
            //
        }  
        
        
        
	}
	
	
	
	
	//main
	/*
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {   
        Configuration conf = new Configuration();  
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();  
          
        if (otherArgs.length != 3)  
        {  
          System.err.println("params:<business_Dir> <review_Dir> <OutDir>");  
          System.exit(1);  
        }  
        Job job = new Job(conf,"QuestionThree");  
        job.setJarByClass(QuestionThree.class);  
        job.setReducerClass(JoinReducer.class);  
        job.setOutputKeyClass(Text.class);  
        job.setOutputValueClass(Text.class);  
        MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class, BusinessMapper.class);  
        MultipleInputs.addInputPath(job, new Path(otherArgs[1]), TextInputFormat.class, ReviewMapper.class);  
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));  
        //job.getConfiguration().set("join.type", otherArgs[3]);  
          
        System.exit(job.waitForCompletion(true) ? 0 : 1);  
    }  
	*/
	
		public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {   
	        Configuration conf = new Configuration();  
	        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();  
	       // final String INTERPATH = "intermediate_output";
	        
	        if (otherArgs.length != 4)  
	        {  
	          System.err.println("params:<review_Dir> <intermediate_output> <business_Dir> <ResultOutDir>");  
	          System.exit(1);  
	        }  
	        /*job 1 setting*/
	        Job job1 = new Job(conf,"JobOne");  
	        job1.setJarByClass(QuestionThree.class);
	        job1.setMapperClass(MapClassJobOne.class);
	        job1.setReducerClass(ReduceClassJobOne.class); 	        
	        job1.setOutputKeyClass(Text.class);  
	        job1.setOutputValueClass(FloatWritable.class);
	        FileInputFormat.addInputPath(job1, new Path(otherArgs[0])); 
	        FileOutputFormat.setOutputPath(job1, new Path(otherArgs[1]));
	        job1.waitForCompletion(true);
	        
	        /*job 2 setting*/
	        Job job2 = new Job(conf,"JobTwo");  
	        job2.setJarByClass(QuestionThree.class);
	        job2.setReducerClass(JoinReducer.class);  
	        job2.setOutputKeyClass(Text.class);  
	        job2.setOutputValueClass(Text.class); 	        
	        MultipleInputs.addInputPath(job2, new Path(otherArgs[1]), TextInputFormat.class, ReviewMapper.class);  
	        MultipleInputs.addInputPath(job2, new Path(otherArgs[2]), TextInputFormat.class, BusinessMapper.class);  
	        FileOutputFormat.setOutputPath(job2, new Path(otherArgs[3]));  
	      	          
	        System.exit(job2.waitForCompletion(true) ? 0 : 1);  
	    }  
	
	

}
