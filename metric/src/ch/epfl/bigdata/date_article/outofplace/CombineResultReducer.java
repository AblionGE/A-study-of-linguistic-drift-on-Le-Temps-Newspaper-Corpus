package ch.epfl.bigdata.outofplace;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Reduce function: For each year pair, combine the "outofplace" result.
 * 					FinalResult = sum of "outofplace" for matched-pair + "maxOutOfPlace" * unmatched-pair	
 * 
 * Key:		year pairs (e.g., 1840-1998)
 * Value:	'out of place' 
 * @author: Tao Lin
 */

public class CombineResultReducer extends Reducer<CombinationKey, Text, NullWritable, Text> {

	private String filesPath = "";
	HashMap<String, Long> countList = new HashMap<String, Long>();
	
	/**
	 * Based on the formula, and calculate the final result
	 */
	public void reduce(CombinationKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		long max = 0;
		long totalOutOfPlace = 0;
		int numOfMatch = 0;
				
		String [] yearSplit = key.getFirstKey().toString().split("_");
		
		for(int i = 0; i < yearSplit.length; i ++)
			if(!countList.containsKey(yearSplit[i]))
				countList.put(yearSplit[i], countLineForFile(context, yearSplit[i]));
								
		for (Text val : values) {
			totalOutOfPlace += Long.parseLong(key.getSecondKey().toString());
			numOfMatch++;
			
			if(Integer.parseInt(key.getSecondKey().toString()) > max)
				max = Long.parseLong(key.getSecondKey().toString());
		}
		
		totalOutOfPlace = totalOutOfPlace + (countList.get(yearSplit[0]) + countList.get(yearSplit[1]) - numOfMatch * 2) * max;
		
		context.write(NullWritable.get(), new Text(yearSplit[0] + "\t" + yearSplit[1] + "\t" + totalOutOfPlace));
		
	}
	
	/**
	 * Instead of using the output of reducer, here we directly read previous result through HDFS, and found out how many words per year. 
	 * @param context
	 * @param year
	 * @return number of word for certain year.
	 * @throws IOException
	 */
	private Long countLineForFile(Context context, String year) throws IOException{
	    filesPath = FileOutputFormat.getOutputPath(context).getParent() + "/rankFrequency"; 
	    
		FileSystem fs = FileSystem.get(context.getConfiguration());
				
		// read file list
		 Path filePaths = new Path(filesPath);   
		 FileStatus stats[] = fs.listStatus(filePaths);   
		 long count = 0;
		 
		 // cope with each file
		 for(int i = 0; i < stats.length; i ++){
			 Path inFile = new Path(stats[i].getPath().toString());  
			 
			 if(inFile.getName().contains(year)){

				 FSDataInputStream fin = fs.open(inFile);  
				 BufferedReader input = new BufferedReader(new InputStreamReader(fin, "UTF-8"));  
				 
				 // cope with current file
				 while (input.readLine() != null) 
					 count ++;
				 
		         // release
				 if (input != null) {  
					 input.close();  
					 input = null;  
				 }  
				 if (fin != null) {  
					 fin.close();  
					 fin = null;  
				 } 
				 
			 }
		 }
	    
		return count;
	}

}
