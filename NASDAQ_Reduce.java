package Andrija.BigData.NASDAQ_Join;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.reduce.*;
import com.sun.tools.javac.util.Context;

import HelperTextClasses.TextPair;

public class NASDAQ_Reduce extends Reducer<Text, TextPair, Text, IntWritable>{

	

   	@Override
   	public void reduce( Text key, Iterable<TextPair>values, Context context){

   		int maxValues = Integer.MIN_VALUE;
   		
   		TextPair maxPair = new TextPair("blah",0);
   		
   		int maxPairSecondField = Integer.parseInt(maxPair.getSecond().toString());

   		for (TextPair a: values){
   			
   			//we need to break up the TextPair and convert the second part of the pair back into an int,
   			//in order to be able to compare the two 
   			
   			
   			//first we will convert the 2nd field into a string
   			// then we will convert that string into an int
   			// if we compare this int to the 2nd fields of the maxPair Textpair and it is higher in value
   			// we will replace this textPair with the new one
   			
   			String temp=a.getSecond().toString();
   			int secondFieldValue=Integer.parseInt(temp);
   			
   			if (secondFieldValue>maxPairSecondField)
   			{
   				
   				
   			}
   			
   			
   			
   			
   			
   			
   			
   			
   		}

   			maxValue = Math.max(maxValue, a.get());
   		}

   		context.write(key, new IntWritable(maxValue));

}
