package ch.epfl.bigdata.outofplace;

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


/*
 * Driver function of Map Reduce. It contains several jobs: 
 *	Job1: GetData -> Split Data and remove the articles from the year it belongs to
 * 	Job2: PrepareData -> Get the reverse rank of word in each file;
 * 	Job3: CalSim -> Calculate the distance between article and each year
 * 	Job4: CombineResult -> Combine the computation result obtained by Job3, i.e., for each article, get its distance with each year.
 * 	Job5: FinalResult -> Normalize the result
 * @author: Tao Lin
 */

public class Driver {

	public static void main(String[] args) throws Exception {
		/* ----------- Initialization -------------- */
		// Configuration
		Configuration conf = new Configuration();
		
		// Set path
	    String[] otherArgs = new String[]{"input/1840","input/data","POutput"};
	    
	    otherArgs[0] = args[0];
	    otherArgs[1] = args[1];
	    otherArgs[2] = args[2];
	    
	    // Delete existing output dir 
	    Path outputPath = new Path(otherArgs[1]);
	    outputPath.getFileSystem(conf).delete(new Path(otherArgs[2]), true);
			
		// ----------- Job rank frequency -------------- //		
	    Job job_getData = Job.getInstance(conf, "OutOfPlace Measure: Get Data");
	    job_getData.setJarByClass(ch.epfl.bigdata.outofplace.Driver.class);
			   						
	    // Set Basic parameters	
		job_getData.setMapperClass(GetDataMapper.class);
		job_getData.setReducerClass(GetDataReducer.class);
						        
		// Set type of output key and value in the map and reduce
        job_getData.setMapOutputKeyClass(Text.class);
        job_getData.setMapOutputValueClass(Text.class);	
        job_getData.setOutputKeyClass(Text.class);
        job_getData.setOutputValueClass(Text.class);
        
		// Set input and output format  
		job_getData.setInputFormatClass(TextInputFormat.class);
        LazyOutputFormat.setOutputFormatClass(job_getData, TextOutputFormat.class);
	
		// To avoid The _SUCCESS files being created in the mapreduce output folder.
        job_getData.getConfiguration().setBoolean("mapreduce.fileoutputcommitter.marksuccessfuljobs", false); 
		
        // Add job_cleanData to the ControlledJob
        ControlledJob ctrljob_getData = new ControlledJob(conf);
        ctrljob_getData.setJob(job_getData);
        
        // Add Input and output path
		FileInputFormat.addInputPath(job_getData, new Path(otherArgs[0]));
		FileInputFormat.addInputPath(job_getData, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job_getData, new Path(otherArgs[2] + "/yearData"));
        
		// ----------- Job rank frequency -------------- //		
	    Job job_prepare = Job.getInstance(conf, "OutOfPlace Measure: Rank Frequency");
	    job_prepare.setJarByClass(ch.epfl.bigdata.outofplace.Driver.class);
								
	    // Set Basic parameters	
		job_prepare.setMapperClass(PrepareDataMapper.class);
		job_prepare.setReducerClass(PrepareDataReducer.class);
		        
		// Setup secondary sort.  
        job_prepare.setPartitionerClass(DefinedPartition.class); 
        job_prepare.setSortComparatorClass(DefinedComparator.class);
        job_prepare.setGroupingComparatorClass(DefinedGroupSort.class);
				        
		// Set type of output key and value in the map and reduce
        job_prepare.setMapOutputKeyClass(CombinationKey.class);
        job_prepare.setMapOutputValueClass(Text.class);	
        job_prepare.setOutputKeyClass(Text.class);
        job_prepare.setOutputValueClass(Text.class);
        
		// Set input and output format  
		job_prepare.setInputFormatClass(TextInputFormat.class);
        LazyOutputFormat.setOutputFormatClass(job_prepare, TextOutputFormat.class);
	
		// To avoid The _SUCCESS files being created in the mapreduce output folder.
        job_prepare.getConfiguration().setBoolean("mapreduce.fileoutputcommitter.marksuccessfuljobs", false); 
		
        // Add job_cleanData to the ControlledJob
        ControlledJob ctrljob_prepare = new ControlledJob(conf);
        ctrljob_prepare.setJob(job_prepare);
        
        // Add reliable relationship
        ctrljob_prepare.addDependingJob(ctrljob_getData);
        
        // Add Input and output path
        FileInputFormat.addInputPath(job_prepare, new Path(otherArgs[0]));
        FileInputFormat.addInputPath(job_prepare, new Path(otherArgs[2] + "/yearData/"));
        FileOutputFormat.setOutputPath(job_prepare, new Path(otherArgs[2] + "/rankFrequency/"));
	      
		// ----------- Job Calculation -------------- //		
	    Job job_CalSim = Job.getInstance(conf, "Calculation outofplace");
	    job_CalSim.setJarByClass(ch.epfl.bigdata.outofplace.Driver.class);
								
	    // Set Basic parameters	
		job_CalSim.setMapperClass(CalMapper.class);
		job_CalSim.setReducerClass(CalReducer.class);
		        
		// Setup secondary sort.  
        job_CalSim.setPartitionerClass(DefinedPartition.class); 
        job_CalSim.setSortComparatorClass(DefinedComparator.class);
        job_CalSim.setGroupingComparatorClass(DefinedGroupSort.class);
				        
		// Set type of output key and value in the map and reduce
        job_CalSim.setMapOutputKeyClass(CombinationKey.class);
        job_CalSim.setMapOutputValueClass(Text.class);	
        job_CalSim.setOutputKeyClass(Text.class);
        job_CalSim.setOutputValueClass(Text.class);
        
		// Set input and output format  
		job_CalSim.setInputFormatClass(TextInputFormat.class);
        LazyOutputFormat.setOutputFormatClass(job_CalSim, TextOutputFormat.class);
	
		// To avoid The _SUCCESS files being created in the mapreduce output folder.
        job_CalSim.getConfiguration().setBoolean("mapreduce.fileoutputcommitter.marksuccessfuljobs", false); 
		
        // Add job_cleanData to the ControlledJob
        ControlledJob ctrljob_CalSim = new ControlledJob(conf);
        ctrljob_CalSim.setJob(job_CalSim);
        
        // Add reliable relationship
        ctrljob_CalSim.addDependingJob(ctrljob_prepare);
        
        // Add Input and output path
        FileInputFormat.addInputPath(job_CalSim, new Path(otherArgs[2] + "/rankFrequency/"));
        FileOutputFormat.setOutputPath(job_CalSim, new Path(otherArgs[2] + "/outofplace/"));
        
		// ----------- Job Calculation -------------- //		
	    Job job_combineResult = Job.getInstance(conf, "get result of outofplace");
	    job_combineResult.setJarByClass(ch.epfl.bigdata.outofplace.Driver.class);
								
	    // Set Basic parameters	
		job_combineResult.setMapperClass(CombineResultMapper.class);
		job_combineResult.setReducerClass(CombineResultReducer.class);
		        
		// Setup secondary sort.  
        job_combineResult.setPartitionerClass(DefinedPartition.class); 
        job_combineResult.setSortComparatorClass(DefinedComparator.class);
        job_combineResult.setGroupingComparatorClass(DefinedGroupSort.class);
				        
		// Set type of output key and value in the map and reduce
        job_combineResult.setMapOutputKeyClass(CombinationKey.class);
        job_combineResult.setMapOutputValueClass(Text.class);	
        job_combineResult.setOutputKeyClass(Text.class);
        job_combineResult.setOutputValueClass(Text.class);
        
		// Set input and output format  
		job_combineResult.setInputFormatClass(TextInputFormat.class);
        LazyOutputFormat.setOutputFormatClass(job_combineResult, TextOutputFormat.class);
	
		// To avoid The _SUCCESS files being created in the mapreduce output folder.
        job_combineResult.getConfiguration().setBoolean("mapreduce.fileoutputcommitter.marksuccessfuljobs", false); 
		
        // Add job_cleanData to the ControlledJob
        ControlledJob ctrljob_combineResult = new ControlledJob(conf);
        ctrljob_combineResult.setJob(job_combineResult);
        
        // Add reliable relationship
        ctrljob_combineResult.addDependingJob(ctrljob_CalSim);
        
        // Add Input and output path
        FileInputFormat.addInputPath(job_combineResult, new Path(otherArgs[2] + "/outofplace/"));
        FileOutputFormat.setOutputPath(job_combineResult, new Path(otherArgs[2] + "/tmpResult/"));
        
		// ----------- Job Get finalResult -------------- //		
	    Job job_finalResult = Job.getInstance(conf, "get final result");
	    job_finalResult.setJarByClass(ch.epfl.bigdata.outofplace.Driver.class);
								
	    // Set Basic parameters	
		job_finalResult.setMapperClass(FinalResultMapper.class);
		job_finalResult.setReducerClass(FinalResultReducer.class);
				        
		// Set type of output key and value in the map and reduce
        job_finalResult.setMapOutputKeyClass(Text.class);
        job_finalResult.setMapOutputValueClass(Text.class);	
        job_finalResult.setOutputKeyClass(Text.class);
        job_finalResult.setOutputValueClass(Text.class);
        
		// Set input and output format  
		job_finalResult.setInputFormatClass(TextInputFormat.class);
        LazyOutputFormat.setOutputFormatClass(job_finalResult, TextOutputFormat.class);
	
		// To avoid The _SUCCESS files being created in the mapreduce output folder.
        job_finalResult.getConfiguration().setBoolean("mapreduce.fileoutputcommitter.marksuccessfuljobs", false); 
		
        // Add job_cleanData to the ControlledJob
        ControlledJob ctrljob_finalResult = new ControlledJob(conf);
        ctrljob_finalResult.setJob(job_finalResult);
        
        // Add reliable relationship
        ctrljob_finalResult.addDependingJob(ctrljob_combineResult);
        
        // Add Input and output path
        FileInputFormat.addInputPath(job_finalResult, new Path(otherArgs[2] + "/tmpResult/"));
        FileOutputFormat.setOutputPath(job_finalResult, new Path(otherArgs[2] + "/finalResult/"));
        
        // ---------- Set the number of reducer ---------//
        
        job_getData.setNumReduceTasks(21);
        job_prepare.setNumReduceTasks(21);
        job_CalSim.setNumReduceTasks(21);
        job_combineResult.setNumReduceTasks(21);
        job_finalResult.setNumReduceTasks(1);
        
        // ------------------ Control ------------------//
        // Main controller, control above two jobs.
        JobControl jobCtrl = new JobControl("myctrl_");
        
        // Add controlJob to the main controller.
        jobCtrl.addJob(ctrljob_getData);
        jobCtrl.addJob(ctrljob_prepare);
        jobCtrl.addJob(ctrljob_CalSim);
        jobCtrl.addJob(ctrljob_combineResult);
        jobCtrl.addJob(ctrljob_finalResult);
                        
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

