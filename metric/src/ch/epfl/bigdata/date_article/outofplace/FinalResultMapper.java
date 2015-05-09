package ch.epfl.bigdata.outofplace;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Map function:
 * key: article
 * value: year	\tab	"outofplace"
 * @author: Tao Lin
 */

public class FinalResultMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String [] lineSplit = value.toString().split("\\s+");

		if(lineSplit[0].length() == 6)
			context.write(new Text(lineSplit[0]), new Text(lineSplit[1] + "\t" + lineSplit[2]));
	}

}
