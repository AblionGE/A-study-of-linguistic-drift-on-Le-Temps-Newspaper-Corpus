package ch.bigdata2015.linguisticdrift.tfidf;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * The reducer for Merging files of Total TF.
 * @author Marc Schaer
 *
 */
public class TfReducerMerge extends Reducer<Text, Text, Text, Iterable<Text>> {

	/**
	 * Setup the reducer.
	 */
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		super.setup(context);
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		super.cleanup(context);
	}

	public void reduce(Text inputKey, Iterable<Text> inputValues,
			Context context) throws IOException, InterruptedException {
		context.write(inputKey, inputValues);
	}

}
