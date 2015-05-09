package ch.epfl.bigdata.linguisticdrift.errorcorrection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Driver {
	
	/**
	 * Main target: Correct spelling errors and resort n-grams sequence.
	 * The following code is the driver function of the map-reduce framework.
	 * 
	 * It consists of 2 jobs:
	 * Job1: correct OCR error
	 * Job2: rank frequency in ascending order.
	 * 
	 * @param args: args[0] -> input path; args[1] -> output path 
	 * @author Tao Lin
	 */

	public static void main(String[] args) throws Exception {	
		
		// ----------- Initialization -------------- //
		// Configuration
		Configuration conf = new Configuration();
		
		// Set path
	    String[] otherArgs = new String[]{"input","POutput"};
	    
	    otherArgs[0] = args[0];
	    otherArgs[1] = args[1];
	    
		// Delete existing output dir 
	    Path outputPath = new Path(otherArgs[1]);
	    outputPath.getFileSystem(conf).delete(new Path(otherArgs[1]), true);

	    if (otherArgs.length != 2) {
	        System.err.println("Usage: bin/hadoop jar pro.jar <input> <output>");
	        System.exit(2);
	    }

		// ----------- Set Job_correct -------------- //		
	    Job job_correct = Job.getInstance(conf, "Spell Error Correction");
	    job_correct.setJarByClass(ch.epfl.bigdata.linguisticdrift.errorcorrection.Driver.class);
			   				
	    // Set Basic parameters	
	    job_correct.setMapperClass(CorrectMapper.class);
	    //job_correct.setCombinerClass(CorrectReducer.class);
	    job_correct.setReducerClass(CorrectReducer.class);
				
		// Set type of output key and value in the map and reduce
	    job_correct.setMapOutputKeyClass(Text.class);
	    job_correct.setMapOutputValueClass(Text.class);	
	    job_correct.setOutputKeyClass(Text.class);
	    job_correct.setOutputValueClass(Text.class);
	    
		// Set input and output format  
	    job_correct.setInputFormatClass(TextInputFormat.class);
        LazyOutputFormat.setOutputFormatClass(job_correct, TextOutputFormat.class);
        
		// To avoid The _SUCCESS files being created in the mapreduce output folder.
        job_correct.getConfiguration().setBoolean("mapreduce.fileoutputcommitter.marksuccessfuljobs", false);
        
        // Add job_cleanData to the ControlledJob
        ControlledJob ctrljob_correct = new ControlledJob(conf);
        ctrljob_correct.setJob(job_correct);
        
        // Add output path
		FileInputFormat.setInputPaths(job_correct, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job_correct, new Path(otherArgs[1] + "/correctData/"));
		
		job_correct.setNumReduceTasks(9);
				
		// ----------- Set Job_rank -------------- //
	    Job job_rank = Job.getInstance(conf, "Rank Frequency");
	    job_rank.setJarByClass(ch.epfl.bigdata.linguisticdrift.errorcorrection.Driver.class);
			   				
	    // Set Basic parameters	
	    job_rank.setMapperClass(RankMapper.class);
	    job_rank.setReducerClass(RankReducer.class);
	    
		// Setup secondary sort.  
	    job_rank.setPartitionerClass(DefinedPartition.class); 
	    job_rank.setSortComparatorClass(DefinedComparator.class);
	    job_rank.setGroupingComparatorClass(DefinedGroupSort.class);
				
		// Set type of output key and value in the map and reduce
	    job_rank.setMapOutputKeyClass(CombinationKey.class);
	    job_rank.setMapOutputValueClass(Text.class);	
	    job_rank.setOutputKeyClass(Text.class);
	    job_rank.setOutputValueClass(Text.class);
	    
		// Set input and output format  
	    job_rank.setInputFormatClass(TextInputFormat.class);
        LazyOutputFormat.setOutputFormatClass(job_rank, TextOutputFormat.class);
        
		// To avoid The _SUCCESS files being created in the mapreduce output folder.
        job_rank.getConfiguration().setBoolean("mapreduce.fileoutputcommitter.marksuccessfuljobs", false);
        
        // Add job_cleanData to the ControlledJob
        ControlledJob ctrljob_rank = new ControlledJob(conf);
        ctrljob_rank.setJob(job_rank);
        
        // Add reliable relationship
        ctrljob_rank.addDependingJob(ctrljob_correct);
        
        // Add Input and output path
        FileInputFormat.addInputPath(job_rank, new Path(otherArgs[1] + "/correctData/"));
        FileOutputFormat.setOutputPath(job_rank, new Path(otherArgs[1] + "/finalResult"));
        
        job_rank.setNumReduceTasks(9);
        		
        // ------------------ Control ------------------ //
        // Main controller, control above two jobs.
        JobControl jobCtrl = new JobControl("myctrl_");
        
        // Add controlJob to the main controller.
        jobCtrl.addJob(ctrljob_correct);
        jobCtrl.addJob(ctrljob_rank);
                        
        // Start thread
        Thread t = new Thread(jobCtrl);
        t.start();

        while (true) {
        	if (jobCtrl.allFinished()){
        		System.out.println(jobCtrl.getSuccessfulJobList());
        		jobCtrl.stop();
        		break;
        	}
        	if (jobCtrl.getFailedJobList().size() > 0){
        		System.out.println(jobCtrl.getFailedJobList());
        		jobCtrl.stop();
        		break;
        	}	
        }      	
	}
	
	
}
