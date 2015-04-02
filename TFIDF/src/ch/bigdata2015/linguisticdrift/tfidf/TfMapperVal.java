package ch.bigdata2015.linguisticdrift.tfidf;

import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.google.gson.Gson;

/**
 * Tf computation to compute the total number of word
 * 
 * @author Jeremy Weber
 *
 */
public class TfMapperVal extends Mapper<LongWritable, Text, Text, Text> {

	HashMap<String, Double> yearFreqMap = new HashMap<String, Double>();

	@Override
	protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.setup(context);
		Configuration conf = context.getConfiguration();
		String stringYearFre = conf.get("yearFreq");
		Gson json = new Gson();
		yearFreqMap = json.fromJson(stringYearFre, yearFreqMap.getClass());
	}

	// Output keys will be words
	private Text outputKey = new Text();

	private Text outputValue = new Text();
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
			//FIXME : I swap the next 2 lines to have them working on the cluster
			int numberOcc = new Integer(token.nextToken());
			String word = token.nextToken();
			// Output of the mapper (Year, Occurences of a word)
			outputKey.set(word);
			double freqTotOcc = yearFreqMap.get(fileName);
			outputValue.set(fileName + "\t" + (double) numberOcc / freqTotOcc);
			context.write(outputKey, outputValue);
		}

	}

}
