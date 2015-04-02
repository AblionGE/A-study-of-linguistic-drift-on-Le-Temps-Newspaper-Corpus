//package ch.bigdata2015.linguisticdrift.tfidf;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 * Tf computation to compute the total number of word
 * 
 * @author Jeremy Weber
 *
 */
public class TfReducerTot extends Reducer<Text, IntWritable, Text, IntWritable> {

	private IntWritable outputValue = new IntWritable();
	private MultipleOutputs<Text, IntWritable> mos;

	/**
	 * Setup the reducer.
	 */
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		super.setup(context);
		mos = new MultipleOutputs<Text, IntWritable>(context);
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		super.cleanup(context);
		mos.close();
	}

	public void reduce(Text inputKey, Iterable<IntWritable> inputValues,
			Context context) throws IOException, InterruptedException {

		int sumOfYear = 0;

		for (IntWritable occ : inputValues) {
			sumOfYear += occ.get();
		}
		outputValue.set(sumOfYear);
		mos.write(inputKey, outputValue, "YearOccurences");
	}

}
