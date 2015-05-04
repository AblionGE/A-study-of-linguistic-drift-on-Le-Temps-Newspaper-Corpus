package ch.epfl.bigdata.outofplace;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/*
 * Map function
 * key:
 * 			firstKey: year pairs (e.g., year-article; article-year) 
 * 			secondKey: "outofplace"
 * value:	word
 */

public class CombineResultMapper extends Mapper<LongWritable, Text, CombinationKey, Text> {
	CombinationKey combinationKey = new CombinationKey();

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String [] lineSplit = value.toString().split("\\s+");
				
		String [] split = lineSplit[2].split("_");

		if(((split[0].length() == 4) && (split[1].length() == 6)) || ((split[0].length() == 6) && (split[1].length() == 4))){
			combinationKey.setFirstKey(new Text(split[0] + "_" + split[1]));
			combinationKey.setSecondKey(new IntWritable(Integer.parseInt(lineSplit[1])));
			context.write(combinationKey, new Text(lineSplit[0]));
			combinationKey.setFirstKey(new Text(split[1] + "_" + split[0]));
			context.write(combinationKey, new Text(lineSplit[0]));
		}
		
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
