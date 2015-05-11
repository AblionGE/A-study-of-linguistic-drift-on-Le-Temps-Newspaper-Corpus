package ch.bigdata2015.linguisticdrift.tfidf;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper to split file by year.
 * @author Marc Schaer
 *
 */
public class SplitByYearTFIDFMapper extends Mapper<Object, Text, IntWritable, Text> {
	
	/**
	 * Mapper.
	 * Split the input and write the year as key and the word + tfidf value as value
	 */
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		String v = value.toString();
		String[] parts = v.split("\t");
		String year = parts[1];
		String tfidf = parts[0] + " " + parts[2];

		context.write(new IntWritable(new Integer(year)), new Text(tfidf));
	}

}
