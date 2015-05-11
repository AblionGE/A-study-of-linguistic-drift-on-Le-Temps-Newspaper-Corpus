package ch.epfl.bigdata.outofplace;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 * Reduce function: Combine small article to be a big article; remove word's frequency of article from the year it belongs to	
 * key:		word
 * value:	modified frequency	 
 * @author: Tao Lin 
 */

public class GetDataReducer extends Reducer<Text, Text, Text, Text> {
	private MultipleOutputs<Text, Text> mos;
	
	/**
	 * Setup function for multiple path output
	 */
	protected void setup(Context context) throws IOException, InterruptedException {	
	    super.setup(context);	    
	    mos = new MultipleOutputs(context);
	}
	
	/**
	 * Reduce function
	 */
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {		
		int [] articleInfoList= {0, 0};
		HashMap<String, String> yearInfoList = new HashMap<String, String>();
		
		// obtain article Information and year information
		for (Text val : values) {
			String [] split = val.toString().split(";");
			
			if(split[0].contains("article")){
				if(articleInfoList[0] != Integer.parseInt(split[1])){
					articleInfoList[0] = Integer.parseInt(split[1]);
					articleInfoList[1] = Integer.parseInt(split[2]);
				}
				else{
					articleInfoList[1] = articleInfoList[1] + Integer.parseInt(split[2]);
				}
			}
			else if(split[0].contains("year")){
				yearInfoList.put(split[1], split[2]);
			}			
		}
		
		// Remove word's frequency of article from the year it belongs to	
		for(String ye : yearInfoList.keySet()){
			int left = Integer.parseInt(yearInfoList.get(ye));
			if(Integer.parseInt(ye) == articleInfoList[0]){				
				left -= articleInfoList[1];
			}
			mos.write(key, new Text(String.valueOf(left)), ye);
		}
		
		if(articleInfoList[0] != 0) 	
			mos.write(key, new Text(articleInfoList[1] + ""), "article");

	}
	
	/**
	 * Cleanup function for multiple path output
	 */
	protected void cleanup(Context context) throws IOException, InterruptedException{
		super.cleanup(context);		
		mos.close();
	}
}
