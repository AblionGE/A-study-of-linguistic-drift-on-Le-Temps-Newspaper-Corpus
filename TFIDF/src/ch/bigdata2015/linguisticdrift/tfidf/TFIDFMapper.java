package ch.bigdata2015.linguisticdrift.tfidf;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper for TF-IDF combination.
 * 
 * @author Marc Schaer
 *
 */
public class TFIDFMapper extends Mapper<Object, Text, Text, Text> {

	/**
	 * Mapper for TFIDF combinator. Map files in format : word year value
	 */
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		String v = value.toString();
		String[] parts = v.split("\t");
		String word = parts[0];
		String str = parts[1] + "\t" + parts[2];

		context.write(new Text(word), new Text(str));
	}
}
