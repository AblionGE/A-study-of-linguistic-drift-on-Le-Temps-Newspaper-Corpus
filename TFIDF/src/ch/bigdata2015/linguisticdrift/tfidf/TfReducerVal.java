package ch.bigdata2015.linguisticdrift.tfidf;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/** Reducer for Tf Val.
 * Using MultipleOutputs to rename output files
 * 
 * @author Marc Schaer
 *
 */
public class TfReducerVal extends Reducer<Text, Text, Text, Text> {

	private MultipleOutputs<Text, Text> mos;

	/**
	 * Setup the reducer.
	 */
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		super.setup(context);
		mos = new MultipleOutputs<Text, Text>(context);
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		super.cleanup(context);
		mos.close();
	}

	public void reduce(Text inputKey, Iterable<Text> inputValues,
			Context context) throws IOException, InterruptedException {

		for (Text occ : inputValues) {
			mos.write(inputKey, occ, "TF");
		}
	}
}
