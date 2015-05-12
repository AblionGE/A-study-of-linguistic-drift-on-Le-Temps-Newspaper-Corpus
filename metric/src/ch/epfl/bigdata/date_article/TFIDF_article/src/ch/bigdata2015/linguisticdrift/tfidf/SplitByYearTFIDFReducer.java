package ch.bigdata2015.linguisticdrift.tfidf;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer to split file by year.
 * Simply write the key and the value
 * 
 * @author Marc Schaer
 *
 */
public class SplitByYearTFIDFReducer extends
		Reducer<IntWritable, Text, IntWritable, Iterable<Text>> {

	public void reduce(IntWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		context.write(key, values);
	}
}
