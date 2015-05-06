package ch.epfl.bigdata.outofplace;

import java.io.IOException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/*
 * Reduce function: map the frequency of word, and obtain word's rank(descending).
 * Output Key: Null 
 * Output Value: word	rank 
 * @author: Tao Lin 
 */

public class PrepareDataReducer extends Reducer<CombinationKey, Text, NullWritable, Text> {
	
	private MultipleOutputs<NullWritable, Text> mos;
	int [] storeInfo;
	
	protected void setup(Context context) throws IOException, InterruptedException {	
	    super.setup(context);
	    mos = new MultipleOutputs(context);
	}
	
	public void reduce(CombinationKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		String file = key.getFirstKey().toString();
		
		// storeInfo use to store the info of count and frequency
		storeInfo = new int[2];
		
		for(Text val: values){
			
			if(storeInfo[1] != Integer.parseInt(key.getSecondKey().toString())){
				storeInfo[0]++;
				storeInfo[1] = Integer.parseInt(key.getSecondKey().toString());
			}
					
			mos.write(NullWritable.get(), new Text(val + "\t" + storeInfo[0]), file);
		}
		
	}
	
	protected void cleanup(Context context) throws IOException, InterruptedException{
		super.cleanup(context);
		mos.close();
	}

}
