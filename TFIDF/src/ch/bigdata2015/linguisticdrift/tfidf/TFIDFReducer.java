//package ch.bigdata2015.linguisticdrift.tfidf;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer for TF-IDF combination.
 * 
 * @author Marc Schär
 *
 */
public class TFIDFReducer extends Reducer<Text, Text, Text, Text> {
	private Text result = new Text();
	private Double idf = 0.0;

	/**
	 * Setup the reducer.
	 */
	protected void setup(Context context) throws IOException,
			InterruptedException {
		super.setup(context);
	}

	/**
	 * Reduce method. Compute the number of year in which each word appears
	 * 
	 * @param key
	 *            : the word
	 * @param values
	 *            : the list of occurrences
	 */
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		ArrayList<Text> cache = new ArrayList<Text>();
		for (Text val : values) {
			String[] parts = val.toString().split("\t");
			if ((parts[0]).equals("0000")) {
				String idfStr = parts[1];
				idf = new Double(idfStr);
			} else {
				cache.add(new Text(val));
			}
		}

		for (Text val : cache) {
			String[] parts = val.toString().split("\t");
			if (!((parts[0]).equals("0000"))) {
				String year = parts[0];
				String tf = parts[1];

				Double tfidf = new Double(tf) * idf;
				result.set(year + "\t" + tfidf);
				context.write(key, result);
			}
		}
	}

	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		super.cleanup(context);
	}
}
