package ch.bigdata2015.linguisticdrift.tfidf;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * Tf computation to compute the total number of word
 * 
 * @author Jeremy Weber
 *
 */
public class TfMapperTot extends Mapper<LongWritable, Text, Text, IntWritable> {

	// Output keys will be words
	private Text outputKey = new Text();

	private IntWritable outputValue = new IntWritable();
	String fileName;

	/**
	 * Map function that outputs : Key : Year Value : occurence for a word
	 */
	public void map(LongWritable inputKey, Text inputValue, Context context)
			throws IOException, InterruptedException {
		fileName = ((FileSplit) context.getInputSplit()).getPath().getName()
				.substring(0, 4);
		StringTokenizer token = new StringTokenizer(inputValue.toString());
		// For every line we take the word and its occurence
		while (token.hasMoreTokens()) {
			// We burn the word

			int numberOcc = new Integer(token.nextToken());
			token.nextToken();

			// Output of the mapper (Year, Occurences of a word)
			outputKey.set(fileName);
			outputValue.set(numberOcc);
			context.write(outputKey, outputValue);
		}

	}

}
