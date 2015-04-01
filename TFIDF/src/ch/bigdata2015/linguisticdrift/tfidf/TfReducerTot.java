import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Tf computation to compute the total number of word
 * 
 * @author Jeremy Weber
 *
 */
public class TfReducerTot extends Reducer<Text, IntWritable, Text, IntWritable> {

	private IntWritable outputValue = new IntWritable();

	public void reduce(Text inputKey, Iterable<IntWritable> inputValues,
			Context context) throws IOException, InterruptedException {

		int sumOfYear = 0;

		for (IntWritable occ : inputValues) {
			sumOfYear += occ.get();
		}
		outputValue.set(sumOfYear);
		context.write(inputKey, outputValue);
	}

}
