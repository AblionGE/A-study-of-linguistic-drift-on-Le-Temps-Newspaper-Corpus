package ch.bigdata2015.linguisticdrift.tfidf;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * The reducer for IFD.
 * 
 * @author Marc Schaer
 *
 */
public class IDFReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {

	private DoubleWritable result = new DoubleWritable();

	/**
	 * Setup the reducer.
	 */
	protected void setup(Context context) throws IOException,
			InterruptedException {
		super.setup(context);
	}

	/**
	 * Reduce method.
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
		//FIXME : 159 should be known from the input directory
		result.set(Math.log((2/sum)));
		context.write(key, result);
	}

	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		super.cleanup(context);
	}
}
