package ch.epfl.bigdata.outofplace;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/*
 * Map function:	
 * 				key:	word
 * 				value:	type;year;frequency	 
 * @author: Tao Lin 
 */

public class GetDataMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// article data
		String folderName = getParentFolderName(context);
		
		String [] split = value.toString().split("\\s+");
		
		if(folderName.matches("^[0-9]*")){
			context.write(new Text(split[0]), new Text("article;" + folderName + ";" + split[1]));
		}
		// year data
		else{
			context.write(new Text(split[0]), new Text("year;" + getFileName(context) + ";" + split[1]));
		}
		
	}
	
	 // Get the name of input file in the correct format. 
	private String getFileName(Context context){
		// Get input file information
		FileSplit splitInfo = (FileSplit) context.getInputSplit();
		String fileName = splitInfo.getPath().getName();
		String [] split = fileName.split("-");
		
		return split[0];
	}
	
	 // Get the name of input file in the correct format. 
	private String getParentFolderName(Context context){
		// Get input file information
		FileSplit splitInfo = (FileSplit) context.getInputSplit();
		String fileName = splitInfo.getPath().getParent().getName();
		return fileName;
	}
	
}
