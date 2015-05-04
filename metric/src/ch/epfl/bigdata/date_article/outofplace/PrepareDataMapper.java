package ch.epfl.bigdata.outofplace;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/*
 * Map function: map the frequency of word, and obtain word's rank(descending). 	
 * key:
 * 			firstKey: fileName
 * 			secondKey: frequency
 * value:	word	 
 * @author: Tao Lin 
 */
public class PrepareDataMapper extends Mapper<LongWritable, Text, CombinationKey, Text> {
	CombinationKey combinationKey = new CombinationKey();
	String[] lineSplit = null;
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String [] lineSplit = value.toString().split("\\s+");
		
		combinationKey.setFirstKey(new Text(getFileName(context)));
		combinationKey.setSecondKey(new IntWritable(Integer.parseInt(lineSplit[1])));
		context.write(combinationKey, new Text(lineSplit[0]));
	}

	//Get the name of input file in the correct format.	
	private String getFileName(Context context){
		// Get input file information
		FileSplit splitInfo = (FileSplit) context.getInputSplit();
		String fileName = splitInfo.getPath().getName();
		String [] split = fileName.split("-");
		
		if(split[0].matches("^[0-9]*")){
			return split[0];
		}
		else{
			return "100000";
		}
	}
}
