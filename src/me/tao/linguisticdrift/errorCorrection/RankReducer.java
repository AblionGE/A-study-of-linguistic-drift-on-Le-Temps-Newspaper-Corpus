package me.tao.linguisticdrift.errorcorrection;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/* Reducer function: write the n-grams to the corresponding file based on the source of the file. 
 * 
 * @param: inputhad ->
 * 						firstKey: fileName  
 * 						secondKey: Frequency of current n-grams;
 * 						Value: n-grams
 * @param: outputValue ->
 * 						what we want
 * @author Tao Lin
 */


public class RankReducer extends Reducer<CombinationKey, Text, IntWritable, Text> {

	private MultipleOutputs<IntWritable, Text> mos;
	String[] valSplit = null;
	
	protected void setup(Context context) throws IOException, InterruptedException {	
	    super.setup(context);
	    mos = new MultipleOutputs(context);
	}
		
	public void reduce(CombinationKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		for (Text val : values){
			mos.write(key.getSecondKey(), val, key.getFirstKey().toString());
		}
	}
	
	protected void cleanup(Context context) throws IOException, InterruptedException{
		super.cleanup(context);
		mos.close();
	}

}
