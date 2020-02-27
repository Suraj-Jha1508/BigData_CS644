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
public class AverageTaxiTime{
	
    //TreeSet declared
	public static TreeSet<MyDataType> airportWithHighestAverage = new TreeSet<MyDataType>();
	public static TreeSet<MyDataType> airportWithLowestAverage = new TreeSet<MyDataType>();
	
	/*
	MyDataType Class
	
	Define:  A user datatype to store airport code <key> and average taxi time <averageD>
	
	*/
	public static class MyDataType implements Comparable<MyDataType> 
	{
		double average;
		String key;

		MyDataType(double average, String key) 
		{
			this.average = average;
			this.key = key;
			  
		}

		@Override
		public int compareTo(MyDataType myDataType) 
		{
			
			if(this.average<=myDataType.average)
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
	 Output: Key: * Airport or Airport <Text>, Value:1 for airport count and taxi time for * Airport <LongWritable>
	 
	 Define: A Mapper class to filter airports and there taxi time (both in and out). * Airport is for taxi time(in or out) of airports and Airport is for number of flight airport has hosted.
	 
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
				
				if (isInteger(column[19]||isInteger(column[20])) 
				{
					Text origin_1 = new Text("* "+column[16]);
					context.write(origin, new LongWritable(column[19]));					
					Text destination_1 = new Text("* "+column[17]);
					context.write(destination, new LongWritable(column[20]));
				}
				Text origin = new Text(column[16]);
				context.write(origin, value_1);
				Text destination = new Text(column[17]);
				context.write(destination, value_1);

		    
            }
		}
    
	/* 
	 Combiner Class:
	
	 Input : Key: * Flight or Flight <Text>, Value:1 <LongWritable>
	 Output: Key: * Flight or Flight <Text>, Value: count of * Flight OR Flight <LongWritable>
	 
	 Define: A Combiner class to count the number of airlines flew or landed from that airport and total taxi time .
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
	 Output: Key: Flight <Text>, Value: average <Text>
	 
	 Define: Reducer Class is calculating Average taxi time on an airport and adding top 3 and least 3 airports to a tree sets of MyDataType(user Define datatype)
	 
	 */
	public static class Reduce extends Reducer<Text, LongWritable, Text, Text> 
	   {

		private DoubleWritable average = new DoubleWritable();

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
				double taxiTime =0.0;
				String airportName = key.toString().split(" ")[1];

				taxiTime= getCount(values);        // delayed airport Count

				hashMap.put(airportName,taxiTime);
			}
			else 
			{
				String airportName = key.toString().split(" ")[0];

				double count = getCount(values);   // number of fly by that particular airport
				double totalTaxiTime = hashMap.get(airportName);
				average.set( totalTaxiTime / count);  //relative frequency (RF) of word B given word A 
				Double averageD = average.get();
                airportWithHighestAverage.add(new MyDataType(averageD, key.toString()));
				airportWithLowestAverage.add(new MyDataType(averageD, key.toString()));
				if (airportWithHighestAverage.size() > 3) 
					{
					airportWithHighestAverage.pollLast(); 
					} 
				if (airportWithLowestAverage.size() > 3) 
					{
					airportWithLowestAverage.pollFirst(); 
					}
			
			} 
		}
			//collecting output 
		protected void cleanup(Context context) throws IOException, InterruptedException 
		{   
		    context.write(new Text("Arport with Highest average:  "),null);
			while (!airportWithHighestAverage.isEmpty()) 
				{
					MyDataType pair = airportWithHighestAverage.pollFirst();
	        		context.write(new Text(pair.key), new Text(Double.toString(pair.average)));
				}
			context.write(new Text("Arport with Lowest average:  "),null);
			while (!airportWithLowestAverage.isEmpty()) 
				{
					MyDataType pair = airportWithLowestAverage.pollLast();
	        		context.write(new Text(pair.key), new Text(Double.toString(pair.average)));
				}
		}

	}

	// Main Method
	
	public static void main(String[] args) throws Exception 
	{   
	    Configuration conf=new Configuration();
		Job wordFreq = new Job(conf,"this Program provide 3 airports with the longest and shortest average taxi time per airport (both in and out)");
		wordFreq.setJarByClass(AverageTaxiTime.class);

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
