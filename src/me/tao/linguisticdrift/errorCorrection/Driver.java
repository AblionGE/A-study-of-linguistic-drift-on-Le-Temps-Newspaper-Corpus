package me.tao.linguisticdrift.errorCorrection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Driver {

	public static void main(String[] args) throws Exception {	
		/* ----------- Job -------------- */
		// Configuration
		Configuration conf = new Configuration();
		
	    Job job = Job.getInstance(conf, "Spell Error Correction");
		job.setJarByClass(me.tao.linguisticdrift.errorCorrection.Driver.class);
		
		// Set path
	    String[] otherArgs = new String[]{"originData","POutput"};
	    
	    otherArgs[0] = args[0];
	    otherArgs[1] = args[1];   
	    
		// Delete existing output dir 
	    Path outputPath = new Path(otherArgs[1]);
	    outputPath.getFileSystem(conf).delete(new Path(otherArgs[1]), true);

	    if (otherArgs.length != 2) {
	        System.err.println("Usage: bin/hadoop jar pro1.jar <input> <output>");
	        System.exit(2);
	    }
	   
		FileInputFormat.setInputPaths(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
				
	    // Set Basic parameters	
		job.setMapperClass(CorrectionMapper.class);
		job.setReducerClass(CorrectionReducer.class);
				
		// Setup secondary sort.  
		job.setPartitionerClass(DefinedPartition.class); 
		job.setSortComparatorClass(DefinedComparator.class);
		job.setGroupingComparatorClass(DefinedGroupSort.class); 
			
		// Set type of output key and value in the map.
		job.setMapOutputKeyClass(CombinationKey.class);
        job.setMapOutputValueClass(Text.class);
		
		// Set input and output format  
        job.setInputFormatClass(TextInputFormat.class);
        //job.setOutputFormatClass(TextOutputFormat.class);
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
        
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
				
		// To avoid The _SUCCESS files being created in the mapreduce output folder.
		job.getConfiguration().setBoolean("mapreduce.fileoutputcommitter.marksuccessfuljobs", false); 
							    
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
}
