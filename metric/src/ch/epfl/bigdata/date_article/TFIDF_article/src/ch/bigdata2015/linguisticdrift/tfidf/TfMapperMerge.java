package ch.bigdata2015.linguisticdrift.tfidf;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * The mapper for Merging files of Total TF.
 * @author Marc Schaer
 *
 */
public class TfMapperMerge extends Mapper<LongWritable, Text, Text, Text> {

	/**
	 * 
	 */
	public void map(LongWritable inputKey, Text inputValue, Context context)
			throws IOException, InterruptedException {
		context.write(new Text("sameKey"), inputValue);
	}

}
