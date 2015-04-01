package ch.bigdata2015.linguisticdrift.tfidf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

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
	private static final int NBOFREDUCERS = 2;

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

		}

		// IDF Parts
		{

			Configuration conf = new Configuration();

			// Delete existing output dir
			Path outputPath = new Path(args[1]);
			outputPath.getFileSystem(conf).delete(outputPath, true);

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
			Path outputPath = new Path(args[1]);
			outputPath.getFileSystem(conf).delete(outputPath, true);

			Path inputPath = new Path(args[0]);

			Job job = Job.getInstance(conf, "TFIDF");

			job.setNumReduceTasks(TFIDF.NBOFREDUCERS);

			job.setJarByClass(TFIDF.class);
			job.setMapperClass(TFIDFMapper.class);
			job.setReducerClass(TFIDFReducer.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

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

		// Recreate files per year
		{
			Configuration conf = new Configuration();

			// Delete existing output dir
			Path outputPath = new Path(args[1]);
			outputPath.getFileSystem(conf).delete(outputPath, true);

			Path inputPath = new Path(args[0]);

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
			if (result != 0) {
				System.exit(result);
			}
		}

		System.exit(0);
	}
}
