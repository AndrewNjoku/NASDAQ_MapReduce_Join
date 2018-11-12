package Andrija.BigData.NASDAQ_Join;



import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import HelperTextClasses.TextPair;



class NASDAQ_Day_Partitioner  extends  Partitioner < Text, TextPair >
	   {
	
	
	
	      @Override
	      public int getPartition(Text key, TextPair value, int numReduceTasks)
	      {
	    	  
	    	  
	    	 // lets break down date into its three components 
	    	  //
	    	  
	         String[] str = key.toString().split("-");
	         
	         String yearMonth= str[0]+str[1];
	         
	         String year = str[0];
	         
	         int yearInt = Integer.parseInt(year);
	         
	         //int yearMonthInt = Integer.parseInt(yearMonth);
	         
	         
	         switch (yearInt) {
	            case 1990:  return 0;
	                     
	            case 1991:  return 1 % numReduceTasks;
	                     
	            case 1992:  return 2 % numReduceTasks;
	                   
	            case 1993:  return 3 % numReduceTasks;
	                     
	            case 1994:  return 4 % numReduceTasks;
	                     
	            case 1995:  return 5 % numReduceTasks;
	                     
	            case 1996:  return 6 % numReduceTasks;
	                     
	            case 1997:   return 7 % numReduceTasks;
	                     
	            case 1998:   return 8 % numReduceTasks;
	                     
	            case 1999:  return 9 % numReduceTasks;
	                     
	            case 2000:   return 10 % numReduceTasks;
                
                
                case 2001:   return 11 % numReduceTasks;
                
                
                case 2002:   return 12 % numReduceTasks;
               
                
                case 2003:   return 13 % numReduceTasks;
                
                
                case 2004:   return 14 % numReduceTasks;
                
                
                case 2005:   return 15 % numReduceTasks;
               
                
                case 2006:  return 16 % numReduceTasks;
                
                
                case 2007:   return 17 % numReduceTasks;
                
                
                case 2008:   return 18 % numReduceTasks;
               
                
                case 2009:   return 19 % numReduceTasks;
                
                case 2011:   return 20 % numReduceTasks;
                
                case 2012:   return 21 % numReduceTasks;
                
			    case 2013:   return 22 % numReduceTasks;
			    
			    case 2014:  return 23 % numReduceTasks;    
	        
	   
	      }
	         
	   return 24 % numReduceTasks;
            }
	   }


	   
	