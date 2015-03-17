package ch.epfl.bigdata.ngram;

import java.io.IOException;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**.
 * Reducer for the ngrams.
 * @author gbrechbu
 *
 */
public class NgramReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	MultipleOutputs<Text, IntWritable> mout;
	
	@Override
	public void setup(Context context) {
		 mout = new MultipleOutputs<Text, IntWritable>(context);
	}
	
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context) 
			throws IOException, InterruptedException {
		int sum = 0;
		Iterator<IntWritable> valuesIt = values.iterator();
		String year = new String();
		
		while (valuesIt.hasNext()) {
			IntWritable value = (IntWritable) valuesIt.next();
			sum += value.get();
		}
		Pattern pattern = Pattern.compile("(\\{\\d*\\})");
		//System.out.println("reduce : " + key.toString());
		Matcher matcher = pattern.matcher(key.toString());
		if (matcher.find()) {
			System.out.println("foud !");
			year = matcher.group(1);
		}
		Text finalKey = new Text(key.toString().replace(year, ""));
		year = year.replace("{", "");
		year = year.replace("}", "");
		System.out.println("Reduce-Year : " + year);
		System.out.println("final key : " + finalKey.toString());
		Path outPath = FileOutputFormat.getOutputPath(context);
		System.out.println("Path : " + outPath.toString());
		//Path finalPath = new Path(outPath.toString() + "/" + year.toString());
		mout.write("Output", finalKey, new IntWritable(sum), year);
	}
	
	@Override
	public void cleanup(Context context) {
		try {
			mout.close();
		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
		}
	}
}
