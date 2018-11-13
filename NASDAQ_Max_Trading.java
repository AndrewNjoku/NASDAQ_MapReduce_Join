package Andrija.BigData.NASDAQ_Join;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import HelperTextClasses.TextPair;

public class NASDAQ_Max_Trading
{
	
	
	
	public static void runJob(String [] input, String output ) throws IOException
	{
	
        
        Path outputPath = new Path(output);
        
		Job job = Job.getInstance();
        
        job.setJarByClass(NASDAQ_Max_Trading.class);
        job.setJobName("Max_Trading_Day_Company");
        
        
        FileInputFormat.setInputPaths(job, StringUtils.join(input, ","));
        FileOutputFormat.setOutputPath(job, outputPath);
        
     
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TextPair.class);
        
        
        
        job.setMapperClass(NASDAQ_Map.class);
        
        
        
        //set the partitioner to break up data chunks into year, combiner will run on the map node and simplify the input to reducer by computing
        //the X highest quntity of money on any given day by year
        
        job.setPartitionerClass(NASDAQ_Day_Partitioner.class);
        job.setCombinerClass(NASDAQ_Reduce.class);
        job.setReducerClass(NASDAQ_Reduce.class);
        
        
        //The number of reducetasks should ofcourse reflect the amount of years. From the sample data provided a rough estimate of this is around 24 years
        job.setNumReduceTasks(24);
			
	}

	
    public static void main( String[] args ) throws Exception {
    	
    	if (args.length !=2) {
    		
    		System.err.println("NASDAQ_Max_Trading <Input path> " + "< output path> ");
    		System.exit(-1);
    	}
    	
    	
    	runJob(input, output);
    	
    
    
        
        
    }
}
