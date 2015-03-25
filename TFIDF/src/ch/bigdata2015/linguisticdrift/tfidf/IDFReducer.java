package ch.bigdata2015.linguisticdrift.tfidf;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 * The reducer for IFD. Compute the number of year in which each word appears
 * 
 * @author Marc Schaer
 *
 */
public class IDFReducer extends Reducer<Text, IntWritable, Text, Text> {

	private Text result = new Text();
	private MultipleOutputs<Text, Text> mos;

	/**
	 * Setup the reducer.
	 */
	protected void setup(Context context) throws IOException,
			InterruptedException {
		super.setup(context);
		mos = new MultipleOutputs<Text, Text>(context);
	}

	/**
	 * Reduce method.
	 * Compute the number of year in which each word appears
	 * 
	 * @param key
	 *            : the word
	 * @param value
	 *            : the list of occurrences
	 */
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable val : values) {
			sum += val.get();
		}
		// FIXME : 159 should be known from the input directory
		result.set("0000 \t" + new DoubleWritable(Math.log((159 / sum))));
		mos.write(key, result, "ITF");
	}

	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		super.cleanup(context);
		mos.close();
	}
}
