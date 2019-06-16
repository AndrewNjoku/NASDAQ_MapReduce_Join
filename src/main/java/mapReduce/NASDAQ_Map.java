package mapReduce;

import java.io.IOException;

import helpers.TextPair;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


//input NASDAQ file 
//


public class NASDAQ_Map extends Mapper<LongWritable, Text, Text, TextPair> {



	@Override
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {


         //Line from Nasdaq report
		 // we are going to use the split(,) method 
		 // need to remmeber this utilises regex so we need to escape regular 
		

		
		    // convert text format so we can perform operations as usual 
		
          	String line = value.toString();

          	
          	// There are Nine values per line as follows (0="NASDAQ",1=Company,2=Date,7=Quantity,8=Closing_Price)
          	// We will require Company - Date - CLosing price
          	// that is = NASDAQ[1] + NASDAQ[2] + [8]*[7]
          	//we will convert both [8] and [7] into integers then multiply them together and pass this,
          	//information to the reducer to coalate
          	
          	
   
          	String[] NASDAQ = line.split(",");
          	
          	//[ breaking up the line into seperate words of the NASDAQ array 

          	int Quantity = Integer.parseInt(NASDAQ[8]);
          	
          	int TradingPriceEnd=Integer.parseInt(NASDAQ[7]);
             
          	int totalFunds= Quantity*TradingPriceEnd;
          	
          	// convert calculated total funds back to string 
          	
          	String Date = NASDAQ[2];
          	
          	
          	
          	//we need to emit a key equal to just date so we have all the values for that date and can 
          	//calculate the highest based on splitting the company + value and computing max
          	//with comnbiner
          	

          	
          	
          	
          	// the textpair class i am using helps when you need a secondry text field to support information
          	//in the key but it is not needed for the sort and reduce phase 
          	
          	if ((Quantity>1)&&(totalFunds>Quantity))
          	{
          		context.write(new Text(Date), new TextPair(NASDAQ[1],totalFunds));
         
          		
          	}
          	
          	else {
          		
          		System.err.println(" There has been some sort of error mate, useless");
          	}
          	
  


          }

     






	}



