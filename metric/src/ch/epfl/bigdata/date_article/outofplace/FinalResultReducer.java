package ch.epfl.bigdata.outofplace;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reduce function: output final result
 * key:	article
 * value: its "outofplace" distance with regard to other years (from 1840 - 1998)
 * @author: Tao Lin
 */

public class FinalResultReducer extends Reducer<Text, Text, NullWritable, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		HashMap<Integer, Long> list = new HashMap<Integer, Long>();
		long max = 0;

		for (Text val : values) {
			String [] split = val.toString().split("\t");
			list.put(Integer.parseInt(split[0]), Long.parseLong(split[1]));
			
			if(Long.parseLong(split[1]) > max)
				max = Long.parseLong(split[1]);
		}
				
		for(Integer li: list.keySet()){
			context.write(NullWritable.get(), new Text(key + "," + li + "," + (list.get(li) * 1.0 / max * 1.0)));
		}
		
	}

}
