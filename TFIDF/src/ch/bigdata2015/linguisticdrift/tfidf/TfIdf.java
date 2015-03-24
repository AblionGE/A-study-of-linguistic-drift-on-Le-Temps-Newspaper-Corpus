package ch.bigdata2015.linguisticdrift.tfidf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * TF-IDF functionality. This code is to find the relevance of words through
 * years
 * 
 * @author Marc Schaer
 *
 */
public class TfIdf {

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

		}

		// IDF Parts
		{
			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf, "IDF");
			//conf.set(Job.NUM_MAPS, "25");

			job.setNumReduceTasks(TfIdf.NBOFREDUCERS);

			job.setJarByClass(TfIdf.class);
			job.setMapperClass(IDFMapper.class);
			job.setReducerClass(IDFReducer.class);
			
			// Delete existing output dir 
		    Path outputPath = new Path(args[1]);
		    outputPath.getFileSystem(conf).delete(new Path(args[1]), true);

			job.setOutputFormatClass(IDFFileOutputFormat.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			
			FileInputFormat.addInputPath(job, new Path(args[0]));
			IDFFileOutputFormat.setOutputPath(job, new Path(args[1]));

			// read all 1-gram files (only the words) then do a wordcount
			// then create a file with all words and the idf value

			// To avoid The _SUCCESS files being created in the mapreduce output
			// folder.
			job.getConfiguration().setBoolean(
					"mapreduce.fileoutputcommitter.marksuccessfuljobs", false);

			int result = job.waitForCompletion(true) ? 0 : 1;
			if (result != 0) {
				System.exit(result);
			}

		}

		// Combination of boths
		{

		}

		System.exit(0);
	}
}
