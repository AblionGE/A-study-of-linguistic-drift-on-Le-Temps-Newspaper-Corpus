package ch.epfl.bigdata.linguisticdrift.errorcorrection;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *  Reducer function: Recount the frequency of word. Since the correction process correct some wrong words, 
 *  				  and same word but with different frequency may appear.
 * 
 * @param: inputValue ->
 * 						Key: ('fileName';'ngram'); 
 * 						Value: Frequency of this ngram
 * 						For this tuple, it may exist duplicate.
 * @param: outputValue ->
 * 						Key: ('fileName';'ngram');
 * 						Value: Frequency of this ngram (correct version)
 * @author Tao Lin
 */

public class CorrectReducer extends Reducer<Text, Text, Text, Text> {	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		int sum = 0;
			
		for (Text val : values) {
			sum += Integer.valueOf(val.toString());
		}	
		
		context.write(key, new Text(String.valueOf(sum)));
		
	}

}
