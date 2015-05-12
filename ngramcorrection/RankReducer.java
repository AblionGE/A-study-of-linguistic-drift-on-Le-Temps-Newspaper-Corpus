package ch.epfl.bigdata.linguisticdrift.errorcorrection;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 *  Reducer function: write the n-grams to the corresponding file based on the source of the file. 
 * 
 * @param: inputhad ->
 * 						firstKey: fileName  
 * 						secondKey: Frequency of current n-grams;
 * 						Value: n-grams
 * @param: outputValue ->
 * 						key: Null
 * 						value: word		frequency
 * @author Tao Lin
 */


public class RankReducer extends Reducer<CombinationKey, Text, NullWritable, Text> {

	private MultipleOutputs<NullWritable, Text> mos;
	String[] valSplit = null;
	
	/**
	 * Setup function for multiple path output!
	 */
	protected void setup(Context context) throws IOException, InterruptedException {	
	    super.setup(context);
	    mos = new MultipleOutputs(context);
	}
		
	/**
	 * Output the result
	 */
	public void reduce(CombinationKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		for (Text val : values){
			mos.write(NullWritable.get(), new Text(val), key.getFirstKey().toString());
		}
	}
	
	/**
	 * Cleanup function for multiple path output!
	 */
	protected void cleanup(Context context) throws IOException, InterruptedException{
		super.cleanup(context);
		mos.close();
	}

}
