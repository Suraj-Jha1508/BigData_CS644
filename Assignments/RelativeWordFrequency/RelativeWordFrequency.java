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
public class RelativeWordFrequency{
	
    //TreeSet declared
	public static TreeSet<MyDataType> sTreeSet = new TreeSet<MyDataType>();
	
	/*
	MyDataType Class
	
	Define:  A user datatype to store pair of word <key> and its relative frequency with previous word <rFreqD>
	
	*/
	public static class MyDataType implements Comparable<MyDataType> 
	{
		double relativeFrequency;
		String key;

		MyDataType(double relativeFrequency, String key) 
		{
			this.relativeFrequency = relativeFrequency;
			this.key = key;
			  
		}

		@Override
		public int compareTo(MyDataType myDataType) 
		{
			
			if(this.relativeFrequency<=myDataType.relativeFrequency)
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
	 Output: Key: * A_word or A_word B_word <Text>, Value:1 <LongWritable>
	 
	 Define: A Mapper class to filter words and pairs.
	 
	 */
	
	
	public static class Map extends Mapper<LongWritable, Text, Text, LongWritable>
		{
                
                private Text key_1 = new Text();
                private LongWritable value_1=new LongWritable(1);

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
			{
	
			    			    
                String line =value.toString().toLowerCase().replaceAll("[^a-z _+]", "");
			    StringTokenizer tokenizer= new StringTokenizer(line);
			    List<String> words = new ArrayList<String>();
			    
			    while(tokenizer.hasMoreElements())
			    {
			    	String word_A = new String(tokenizer.nextToken());
			    	word_A = word_A.trim();
			    	if (word_A.length()<3) continue;
			    	words.add(word_A);
			    }
			    for (int i=0;i<words.size();i++)
				{
					key_1.set("*"+" "+words.get(i).trim());
					//collecting output
        	    	context.write(key_1, value_1);
				}
				
			    for (int i=1;i<words.size();i++)
			    {   
			        key_1.set(words.get(i-1).trim()+" "+words.get(i).trim());
					//collecting output
			    	context.write(key_1,value_1);
			    	
			    }

		    }

		}
    
	/* 
	 Combiner Class:
	
	 Input : Key: * A_word or A_word B_word <Text>, Value:1 <LongWritable>
	 Output: Key: * A_word or A_word B_word <Text>, Value: count of A_word OR count of pair:A_word and B_word <LongWritable>
	 
	 Define: A Combiner class to count the number of words and pairs
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
	
	 Input : Key: * A_word or A_word B_word <Text>, Value: count of A_word OR count of pair:A_word and B_word <LongWritable>
	 Output: Key: A_word  B_word <Text>, Value: relativeFrequency <Text>
	 
	 Define: Reducer Class is calculating Relative frequency and adding top 100 pair to a tree set of MyDataType(user Define datatype)
	 
	 */
	public static class Reduce extends Reducer<Text, LongWritable, Text, Text> 
	   {

		private DoubleWritable rFrequency = new DoubleWritable();

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
				double wordCount =0.0;
				String A_word = key.toString().split(" ")[1];

				wordCount= getCount(values);        // A word Count

				hashMap.put(A_word,wordCount);
			}
			else 
			{
				String A_word = key.toString().split(" ")[0];

				double count = getCount(values);   // A B pair Count
				double countOfA = hashMap.get(A_word);
				rFrequency.set((double) count / countOfA);  //relative frequency (RF) of word B given word A 
				Double rFreqD = rFrequency.get();

					if(rFreqD !=1.0d)
						{
							sTreeSet.add(new MyDataType(rFreqD, key.toString()));
							if (sTreeSet.size() > 100) 
							{
								sTreeSet.pollLast(); 
							} 
						}
			
			} 
		}
			//collecting output 
		protected void cleanup(Context context) throws IOException, InterruptedException 
		{
			while (!sTreeSet.isEmpty()) 
				{
					MyDataType pair = sTreeSet.pollFirst();
	        		context.write(new Text(pair.key), new Text(Double.toString(pair.relativeFrequency)));
				}
		}

	}
	
	// Main Method
	
	public static void main(String[] args) throws Exception 
	{   
	    Configuration conf=new Configuration();
		Job wordFreq = new Job(conf,"this Program provide relative frequency of a word");
		wordFreq.setJarByClass(RelativeWordFrequency.class);

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
