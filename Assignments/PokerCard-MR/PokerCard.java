import java.io.IOException;
import java.util.ArrayList;

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


public class PokerCard {
//mapper function
  public static class cardMapper
    extends Mapper<LongWritable, Text, Text, IntWritable>{
	
	@Override	
    public void map(LongWritable key, Text values, Context context)
    	throws IOException, InterruptedException{	
      String line = values.toString();
      String[] split = line.split(",");
      context.write(new Text(split[0]), new IntWritable(Integer.parseInt(split[1])));
    }
  }

//reducer function
  public static class cardReducer
    extends Reducer<Text, IntWritable, Text, IntWritable>{   
    
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
    	
		ArrayList<Integer> nums = new ArrayList<Integer>();
		
    	int sum = 0;
    	int temp = 0;
		
    	for (IntWritable val : values) {
    		sum+= val.get();
    		temp = val.get();
    		nums.add(temp);
    	}
   
    	if(sum < 91){
    		for (int i = 1;i <= 13;i++){
    			if(!nums.contains(i))
    				 context.write(key, new IntWritable(i));
    		}
    	}
    }    
  }

//main function
  public static void main(String[] args) throws Exception {
	
	
    Configuration conf = new Configuration();
    Job job = new Job(conf, "Looking for missing Poker Cards");
	
    job.setJarByClass(PokerCard.class);
	
	FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	
	// configure mapper and reducer
    job.setMapperClass(cardMapper.class);
    job.setReducerClass(cardReducer.class);
	
    job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(IntWritable.class);

		
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}