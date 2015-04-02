package ch.bigdata2015.linguisticdrift.tfidf;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.google.gson.Gson;

/**
 * TF-IDF functionality. This code is to find the relevance of words through
 * years
 * 
 * @author Marc Schaer and Jeremy Weber
 *
 */
public class TFIDF {

	/**
	 * Number of reducers.
	 */
	private static final int NBOFREDUCERS = 50;

	/**
	 * Main function.
	 * 
	 * @param args
	 *            standard args
	 * @throws Exception
	 *             standard Exceptions
	 */
	public static void main(String[] args) throws Exception {
		// TF Parts
		{
			// Delete existing output dir
			Configuration conf = new Configuration();
			Path outputPathTmp = new Path(args[1] + "-TotOccurenceYear");
			outputPathTmp.getFileSystem(conf).delete(outputPathTmp, true);
			Path outputPath = new Path(args[1]);
			outputPath.getFileSystem(conf).delete(outputPath, true);

			// create Hadoop job
			Job job = Job.getInstance();
			job.setJarByClass(TFIDF.class);
			job.setJobName("TF1");
			job.setNumReduceTasks(TFIDF.NBOFREDUCERS);

			// set mapper/reducer classes
			job.setMapperClass(TfMapperTot.class);
			job.setReducerClass(TfReducerTot.class);
			job.setOutputFormatClass(TFTotFileOutputFormat.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);

			// define input and output folders
			FileInputFormat.addInputPath(job, new Path(args[0]));
			TFTotFileOutputFormat.setOutputPath(job, new Path(args[1] + "-TotOccurenceYear"));

			job.getConfiguration().setBoolean(
					"mapreduce.fileoutputcommitter.marksuccessfuljobs", false);
			
			// launch job with verbose output and wait until it finishes
			job.waitForCompletion(true);

			// ///////////
			// Second MapReduce in order to compute the tf value
			// //////////
			Gson json = new Gson();
			// Create a HashMap that links a year with its total number of words
			HashMap<String, Double> yearFreqMap = new HashMap<String, Double>();
			try {
				Path pt = new Path(args[1] + "-TotOccurenceYear/YearOccurences");
				FileSystem fs = FileSystem.get(conf);
				BufferedReader br = new BufferedReader(new InputStreamReader(
						fs.open(pt)));
				String line;
				line = br.readLine();
				while (line != null) {
					String[] yearFreq = line.split("\t");
					yearFreqMap.put(yearFreq[0], new Double(yearFreq[1]));
					line = br.readLine();
				}
				br.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
			// Transform the hashMap into JSON format
			String mapToString = json.toJson(yearFreqMap);
			// Save the HashMap in order to have acces in the mapper
			conf.set("yearFreq", mapToString);
			
			/*
			 * Start the second Map
			 */
			// create Hadoop job
			Job job2 = new Job(conf);
			job2.setJarByClass(TFIDF.class);
			job2.setJobName("TF2");
			job2.setNumReduceTasks(TFIDF.NBOFREDUCERS);

			// set mapper/reducer classes
			job2.setMapperClass(TfMapperVal.class);
			job2.setReducerClass(TfReducerVal.class);

			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(Text.class);

			// define input and output folders
			FileInputFormat.addInputPath(job2, new Path(args[0]));
			FileOutputFormat.setOutputPath(job2, new Path(args[1]
					+ "-tmpTFIDF-TF"));
			
			job2.getConfiguration().setBoolean(
					"mapreduce.fileoutputcommitter.marksuccessfuljobs", false);

			// launch job with verbose output and wait until it finishes
			job2.waitForCompletion(true);
		}

		// IDF Parts
		{

			Configuration conf = new Configuration();

			// Delete existing output dir
			Path outputPath = new Path(args[1] + "-tmpTFIDF-IDF");
			// outputPath.getFileSystem(conf).delete(outputPath, true);

			Path inputPath = new Path(args[0]);
			conf.setLong("numOfFiles", inputPath.getFileSystem(conf)
					.getContentSummary(inputPath).getFileCount());

			Job job = Job.getInstance(conf, "IDF");

			job.setNumReduceTasks(TFIDF.NBOFREDUCERS);

			job.setJarByClass(TFIDF.class);
			job.setMapperClass(IDFMapper.class);
			job.setReducerClass(IDFReducer.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);

			FileInputFormat.addInputPath(job, inputPath);
			FileOutputFormat.setOutputPath(job, outputPath);

			// To avoid The _SUCCESS files being created in the mapreduce output
			// folder.

			job.getConfiguration().setBoolean(
					"mapreduce.fileoutputcommitter.marksuccessfuljobs", false);

			int result = job.waitForCompletion(true) ? 0 : 1;
			if (result != 0) {
				System.exit(result);
			}

		}

		// Compute TFIDF
		{
			// Combination of boths with creating an input file with format
			// word year TFIDF
			Configuration conf = new Configuration();

			// Delete existing output dir
			Path outputPath = new Path(args[1] + "-tmpCompute");
			outputPath.getFileSystem(conf).delete(outputPath, true);

			Path inputPath1 = new Path(args[1] + "-tmpTFIDF-IDF");
			Path inputPath2 = new Path(args[1] + "-tmpTFIDF-TF");

			Job job = Job.getInstance(conf, "TFIDF");

			job.setNumReduceTasks(TFIDF.NBOFREDUCERS);

			job.setJarByClass(TFIDF.class);
			job.setMapperClass(TFIDFMapper.class);
			job.setReducerClass(TFIDFReducer.class);

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			FileInputFormat.addInputPath(job, inputPath1);
			FileInputFormat.addInputPath(job, inputPath2);
			FileOutputFormat.setOutputPath(job, outputPath);

			// To avoid The _SUCCESS files being created in the mapreduce output
			// folder.
			job.getConfiguration().setBoolean(
					"mapreduce.fileoutputcommitter.marksuccessfuljobs", false);

			int result = job.waitForCompletion(true) ? 0 : 1;
			if (result != 0) {
				System.exit(result);
			}

		}

		// Recreate files per year
		{
			Configuration conf = new Configuration();

			// Delete existing output dir
			Path outputPath = new Path(args[1]);

			Path inputPath = new Path(args[1] + "-tmpCompute");

			Job job = Job.getInstance(conf, "TFIDFMerge");

			job.setNumReduceTasks(TFIDF.NBOFREDUCERS);

			job.setJarByClass(TFIDF.class);
			job.setMapperClass(SplitByYearTFIDFMapper.class);
			job.setReducerClass(SplitByYearTFIDFReducer.class);

			// job.setOutputFormatClass(IDFFileOutputFormat.class);
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(Text.class);
			job.setOutputFormatClass(TFIDFFileOutputFormat.class);

			FileInputFormat.addInputPath(job, inputPath);
			TFIDFFileOutputFormat.setOutputPath(job, outputPath);

			// To avoid The _SUCCESS files being created in the mapreduce output
			// folder.
			job.getConfiguration().setBoolean(
					"mapreduce.fileoutputcommitter.marksuccessfuljobs", false);

			int result = job.waitForCompletion(true) ? 0 : 1;
			
			/*
			 * Delete output Folder
			 */
			Path computePath = new Path(args[1] + "-tmpCompute");
			Path IDFPath = new Path(args[1] + "-tmpTFIDF-IDF");
			Path TFPath = new Path(args[1] + "-tmpTFIDF-TF");
			computePath.getFileSystem(conf).delete(computePath, true);
			IDFPath.getFileSystem(conf).delete(IDFPath, true);
			TFPath.getFileSystem(conf).delete(TFPath, true);
			
			if (result != 0) {
				System.exit(result);
			}
		}

		System.exit(0);
	}
}
