package ch.bigdata2015.linguisticdrift.tfidf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

/**
 * TF-IDF functionality.
 * This code is to find the relevance of words through years
 * @author Marc Schaer
 *
 */
public class TfIdf {
	
	/**
	 * Number of reducers.
	 */
	private static final int NBOFREDUCERS = 25;

	/**
	 * Main function.
	 * @param args standard args
	 * @throws Exception standard Exceptions
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "TF-IDF");
		conf.set(Job.NUM_MAPS, "25");

		job.setNumReduceTasks(TfIdf.NBOFREDUCERS);

		job.setJarByClass(TfIdf.class);
		
		//TF Parts
		
		//IDF Parts
		
		
		// To avoid The _SUCCESS files being created in the mapreduce output folder.
		job.getConfiguration().setBoolean("mapreduce.fileoutputcommitter.marksuccessfuljobs", false);
		
		int result = job.waitForCompletion(true) ? 0 : 1;

		System.exit(result);
	}
}
