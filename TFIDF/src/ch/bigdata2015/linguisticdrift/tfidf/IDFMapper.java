package ch.bigdata2015.linguisticdrift.tfidf;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * The Mapper takes 1-grams and return each word with the value one to indicate that the word appears in the year.
 * @author Marc Schaer
 *
 */
public class IDFMapper extends Mapper<Object, Text, Text, IntWritable> {

	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();

	/**
	 * Map function.
	 * @param key : the word
	 * @param value
	 * @param context
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		StringTokenizer itr = new StringTokenizer(value.toString());
		while (itr.hasMoreTokens()) {
			String w = itr.nextToken();
			//Works only with 1-gram
			if (!w.matches("[0-9]+")) {
				word.set(w);
				context.write(word, one);
			}
		}
	}
}
