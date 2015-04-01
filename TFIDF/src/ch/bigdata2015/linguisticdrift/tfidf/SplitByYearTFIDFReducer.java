package ch.bigdata2015.linguisticdrift.tfidf;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer to split file by year.
 * 
 * @author Marc Schaer
 *
 */
public class SplitByYearTFIDFReducer extends
		Reducer<IntWritable, Iterable<Text>, IntWritable, Text> {

	public void reduce(IntWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		for (Text t : values) {
			context.write(key, t);
		}
	}
}
