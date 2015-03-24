package me.tao.linguisticdrift.errorCorrection;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class CorrectionReducer extends Reducer<CombinationKey, Text, IntWritable, Text> {
	
	private MultipleOutputs mos;
	String[] valSplit = null;
	
	protected void setup(Context context) throws IOException, InterruptedException {	
	    super.setup(context);
	    mos = new MultipleOutputs(context);
	}

	public void reduce(CombinationKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		
		for (Text val : values){
			valSplit = val.toString().split(","); 
						
			mos.write(key.getFirstKey(), new Text(valSplit[1].replace(")", "")), generateFileName(valSplit[0].replace("(", "")));
		}
	}
	
	private String generateFileName(String fileName){
		
		String REGEX = "-.-.*";
		Pattern pattern = Pattern.compile(REGEX);
		Matcher mat = pattern.matcher(fileName);
		
		return mat.replaceAll("");
	}
	
	protected void cleanup(Context context)
			throws IOException, InterruptedException{
	super.cleanup(context);
	mos.close();
	}
 
}
