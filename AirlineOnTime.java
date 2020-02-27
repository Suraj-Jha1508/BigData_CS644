//jdk libraries

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

//hadoop conf libraries

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

//hadoop mapreduce libraries

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//main class
public class AirlineOnTime{
	
    //TreeSet declared
	public static TreeSet<MyDataType> flightWithHighestProbablity = new TreeSet<MyDataType>();
	public static TreeSet<MyDataType> flightWithLowestProbablity = new TreeSet<MyDataType>();
	
	/*
	MyDataType Class
	
	Define:  A user datatype to store Airline unique code <key> and its delayed probablity <probablityD>
	
	*/
	public static class MyDataType implements Comparable<MyDataType> 
	{
		double probablity;
		String key;

		MyDataType(double probablity, String key) 
		{
			this.probablity = probablity;
			this.key = key;
			  
		}

		@Override
		public int compareTo(MyDataType myDataType) 
		{
			
			if(this.probablity<=myDataType.probablity)
			{
				return 1;
			}
			else 
			{
				return -1;
			}
			
		}
	}
	
	
	/* 
	 Map Class:
	
	 Input : Key: some number <LongWritable>, Value: sentance <Text>
	 Output: Key: * Flight or Flight <Text>, Value:1 <LongWritable>
	 
	 Define: A Mapper class to filter delayed flights. *Flight is for delayed flights and Flight is for counting number of alirlines flew.
	 
	 */
	
	
	public static class Map extends Mapper<LongWritable, Text, Text, LongWritable>
		{
                
                private LongWritable value_1=new LongWritable(1);
				public static boolean isInteger(String s) {
			        boolean isValidInteger = false;
			    try {
				    Integer.parseInt(s);
				    isValidInteger = true;
			    } catch (NumberFormatException ex)
			    {
			     }
			    return isValidInteger;
		        }

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
			{
	
			    			    
                String column[] =value.toString().split(",");
			    String flight =column[8]
				
				if (isInteger(column[14]||isInteger(column[15])) {
				if (Integer.parseInt(column[14]) >= 10||Integer.parseInt(column[15]) >= 10) 
				{
					Text delayedCarrier = new Text("* "+flight);
					context.write(delayedCarrier, value_1);
				}
				Text countTotFlights = new Text(flight);
				context.write(countTotFlights, value_1);

		    }

		}
    
	/* 
	 Combiner Class:
	
	 Input : Key: * Flight or Flight <Text>, Value:1 <LongWritable>
	 Output: Key: * Flight or Flight <Text>, Value: count of * Flight OR Flight <LongWritable>
	 
	 Define: A Combiner class to count the number of airlines delayed and number of airlines flew.
	 */
	
	public static class Combiner extends Reducer<Text, LongWritable, Text, LongWritable>
		{
			private LongWritable value_1=new LongWritable();
			
		    public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException
			{
				long sum = 0;
				
				for (LongWritable val : values)
					{
						sum = sum + val.get();
					}
				
				value_1.set(sum);
				
				//collecting output
				context.write(key, value_1);
			}

		}
    
	/* 
	 Reducer Class:
	
	 Input : Key: * Flight or Flight <Text>, Value: count of * Flight OR Flight <LongWritable>
	 Output: Key: Flight <Text>, Value: probablity <Text>
	 
	 Define: Reducer Class is calculating Probablity of delayed flights and adding top 3 and least 3 flights to a tree sets of MyDataType(user Define datatype)
	 
	 */
	public static class Reduce extends Reducer<Text, LongWritable, Text, Text> 
	   {

		private DoubleWritable probablity = new DoubleWritable();

		private HashMap<String,Double> hashMap = new HashMap<String,Double>();
		
		// getCount Method count the number of value in values
		private double getCount(Iterable<LongWritable> values) 
		{
			double count = 0;
			
			for (LongWritable value : values) 
			{
				count += value.get();
			}
			return count;
		}

		public void reduce(Text key, Iterable<LongWritable> values, Context context)
			throws IOException, InterruptedException 
			{

			if (key.toString().split(" ")[0].equals("*")) 
			{
				double flightCount =0.0;
				String flightName = key.toString().split(" ")[1];

				flightCount= getCount(values);        // delayed flight Count

				hashMap.put(flightName,flightCount);
			}
			else 
			{
				String flightName = key.toString().split(" ")[0];

				double count = getCount(values);   // number of fly by that particular flight
				double countDelay = hashMap.get(flightName);
				probablity.set( countDelay / count);  //relative frequency (RF) of word B given word A 
				Double probablityD = probablity.get();
                flightWithHighestProbablity.add(new MyDataType(probablityD, key.toString()));
				flightWithLowestProbablity.add(new MyDataType(probablityD, key.toString()));
				if (flightWithHighestProbablity.size() > 3) 
					{
					flightWithHighestProbablity.pollLast(); 
					} 
				if (flightWithLowestProbablity.size() > 3) 
					{
					flightWithLowestProbablity.pollFirst(); 
					}
			
			} 
		}
			//collecting output 
		protected void cleanup(Context context) throws IOException, InterruptedException 
		{   
		    context.write(new Text("Airline with Highest delay Probablity:  "),null);
			while (!flightWithHighestProbablity.isEmpty()) 
				{
					MyDataType pair = flightWithHighestProbablity.pollFirst();
	        		context.write(new Text(pair.key), new Text(Double.toString(pair.probablity)));
				}
			context.write(new Text("Airline with Lowest delay Probablity:  "),null);
			while (!flightWithLowestProbablity.isEmpty()) 
				{
					MyDataType pair = flightWithLowestProbablity.pollLast();
	        		context.write(new Text(pair.key), new Text(Double.toString(pair.probablity)));
				}
		}

	}

	// Main Method
	
	public static void main(String[] args) throws Exception 
	{   
	    Configuration conf=new Configuration();
		Job wordFreq = new Job(conf,"this Program provide 3 airlines with the highest and lowest probability");
		wordFreq.setJarByClass(AirlineOnTime.class);

		wordFreq.setMapperClass(Map.class);
		wordFreq.setCombinerClass(Combiner.class);
		wordFreq.setReducerClass(Reduce.class);
		
		FileInputFormat.addInputPath(wordFreq, new Path(args[0]));
		FileOutputFormat.setOutputPath(wordFreq, new Path(args[1]));

		wordFreq.setMapOutputKeyClass(Text.class);
		wordFreq.setMapOutputValueClass(LongWritable.class);
		wordFreq.setOutputKeyClass(Text.class);
		wordFreq.setOutputValueClass(LongWritable.class);

		wordFreq.waitForCompletion(true);

	}

}
