package Andrija.BigData.NASDAQ_Join;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import HelperTextClasses.TextPair;

public class NASDAQ_Reduce extends Reducer<Text, TextPair, Text, TextPair>{

	

   	@Override
   	public void reduce( Text key, Iterable<TextPair>values, Context context) throws IOException, InterruptedException{
   		
   		//deliverables: we will have a year and 365 values past through each individual reduce function, what does this mean?
   		//we need to sort these results with the highest amount of money for each individual year at the top and the following days 
   		// in descending order
   		//to this will need to implement a comparator function and write to context however many of the top performers output for each year
   		// for example if we want the top 20 performers for any company on any day of any particular year we will have to compare all values with the maximum 
   		//values on each interation bubbling to the top and values falling outside of the collection size being dropped.

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
   			
   			if (secondFieldValue>=maxPairSecondField)
   			{
   				maxPair = a;
   				
   			}
   			
   			context.write(key, maxPair);
 	
   		}

   	
   		}

   		

}
