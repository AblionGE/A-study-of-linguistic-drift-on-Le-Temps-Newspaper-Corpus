package ch.epfl.bigdata.linguisticdrift.errorcorrection;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *  Mapper function: correct spelling error.
 * 
 * @param: inputValue ->
 * 						('fileName';'ngram') (its frequency)
 * @param: outputValue ->
 * 						(name of the source file, correct n-grams	frequency)
 * @param: outputKey ->
 * 						combinationKey.getFirstKey() -> fileName; 
 * 						combinationKey.getSecondKey() -> the frequency of this n-grams
 * @author Tao Lin
 */

public class RankMapper extends Mapper<LongWritable, Text, CombinationKey, Text> {
	
	private CombinationKey combinationKey = new CombinationKey();
	String[] lineSplit = null;

	/**
	 * Map function: Use 'CombinationKey' to implement Secondary sort. 
	 * 				Rank both of word and its frequency in ascending order.
	 */
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
       	// Get lineSplit 	
		String line = value.toString();
		lineSplit = line.split("\\s+");
		
		String [] split = lineSplit[0].split(";");
						
		combinationKey.setFirstKey(new Text(split[0]));
		combinationKey.setSecondKey(new IntWritable(Integer.valueOf(lineSplit[1])));
		
		context.write(combinationKey, new Text(split[1] + "\t" + lineSplit[1]));
						
	}

}
