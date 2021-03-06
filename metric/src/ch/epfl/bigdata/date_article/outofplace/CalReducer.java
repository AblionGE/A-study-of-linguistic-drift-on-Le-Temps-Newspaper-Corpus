package ch.epfl.bigdata.outofplace;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reduce function: Same word but coming from different file with different rank will combine together, and then calculate their "out of place". 
 * Key:
		word
 * Value:
 * 		'out of place' 	\tab	year1_year2
 * @author: Tao Lin
 */

public class CalReducer extends Reducer<CombinationKey, Text, Text, Text> {
	/**
	 * For each word, it will contain a list of <year, rank>.
	 * Thus, for this reducer, it will compute 'out of place' between each year, and output the result.
	 */
	public void reduce(CombinationKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		String word = key.getFirstKey().toString();
		
		HashMap<Integer, Integer> list = new HashMap<Integer, Integer>();
		
		// Receive whole information
		for (Text val : values) {
			list.put(Integer.parseInt(key.getSecondKey().toString()), Integer.parseInt(val.toString()));
		}
		
		// calculate one word's "out of place" between two different years.		
		List<Integer> keylist = new ArrayList<Integer>();
		keylist.addAll(list.keySet());
				
		for(int i = 0; i < keylist.size(); i ++)
			for(int j = i + 1; j < keylist.size(); j ++){				
				int result = Math.abs(list.get(keylist.get(i)) - list.get(keylist.get(j)));
				context.write(new Text(word), new Text(String.valueOf(result) + "\t" + keylist.get(i) + "_" + keylist.get(j)));
			}
		
	}
	
}
