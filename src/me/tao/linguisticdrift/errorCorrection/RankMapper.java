package me.tao.linguisticdrift.errorcorrection;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/* Mapper function: correct spelling error.
 * 
 * @param: inputValue ->
 * 						('fileName';'ngram') (its frequency)
 * @param: outputValue ->
 * 						(name of the source file, correct n-grams)
 * @param: outputKey ->
 * 						combinationKey.getFirstKey() -> fileName; 
 * 						combinationKey.getSecondKey() -> the frequency of this n-grams
 * @author Tao Lin
 */

public class RankMapper extends Mapper<LongWritable, Text, CombinationKey, Text> {
	
	private CombinationKey combinationKey = new CombinationKey();
	
	String[] lineSplit = null;

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
       	// Get lineSplit 	
		String line = value.toString();
		lineSplit = line.split("\\s+");
		
		String [] keyLine = lineSplit[0].split(";");
				
		combinationKey.setFirstKey(new IntWritable(Integer.valueOf(keyLine[0])));
		combinationKey.setSecondKey(new IntWritable(Integer.valueOf(lineSplit[1])));
		
		context.write(combinationKey, new Text(keyLine[1]));
						
	}

}
