package ch.epfl.bigdata.outofplace;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/*
 * Map function:
 * key:
 * 			firstKey: word
 * 			secondKey: fileName
 * value:	rank of the word
 * @author: Tao Lin
 */
public class CalMapper extends Mapper<LongWritable, Text, CombinationKey, Text> {

	CombinationKey combinationKey = new CombinationKey();
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String [] lineSplit = value.toString().split("\\s+");
		
		combinationKey.setFirstKey(new Text(lineSplit[0]));
		combinationKey.setSecondKey(new IntWritable(Integer.parseInt(getFileName(context))));
		context.write(combinationKey, new Text(lineSplit[1]));	
	}
	
	
	 // Get the name of input file in the correct format. 
	private String getFileName(Context context){
		// Get input file information
		FileSplit splitInfo = (FileSplit) context.getInputSplit();
		String fileName = splitInfo.getPath().getName();
		String [] split = fileName.split("-");
		
		return split[0];
	}

}
