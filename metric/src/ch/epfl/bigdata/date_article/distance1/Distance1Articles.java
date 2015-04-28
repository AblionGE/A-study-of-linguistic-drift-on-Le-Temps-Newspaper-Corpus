package ch.epfl.bigdata;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.util.List;

/**
 * Computes a simple distance between a subset of articles from a year with the years from the whole corpus.
 * The distance is based on the common words used.
 * 
 * @author Cynthia, Farah
 * 
 */

public class Distance1Articles {

	private static final int NUM_REDUCERS = 25;

	/**
	 * Mapper: takes a directory with the n-gram files for the subset of articles and a directory with all n-gram files as
	 * input.
	 * For the year of the articles y1 and for each words in y1, it returns a tuple for
	 * each y2 in [1840,1998]: (y1:y2, word).
	 * For each year y2 and each words in y2, it returns a tuple: (y1:y2, word).
	 * 
	 * @author Cynthia
	 * 
	 */
	private static class CDistanceMapper extends
	Mapper<LongWritable, Text, Text, Text> {

		private final int firstYear = 1840;
		private final int lastYear = 1998;
		private String articlesDir;
		private String yearsDir;
		
		/**
		 * Setup the names of both input directories to differentiate words coming from the subset of articles and words coming from the corpus.
		 */
		@Override
		public void setup(Context context) throws IOException {
			String[] path1 = context.getConfiguration().get("inputDirArticles").split("/");
			String[] path2 = context.getConfiguration().get("inputDirTotalYears").split("/");
			articlesDir = path1[path1.length - 1];
			yearsDir = path2[path2.length - 1];
		}

		/**
		 * The function extracts the name of the file from which the line comes and determine the year corresponding to it.
		 * If the line comes from the subset of articles, then a key-value pair is returned for each year in the corpus.
		 * If the line comes from the corpus, a single key-value pair is returned.
		 */
		@Override
		public void map(LongWritable key, Text line, Context context)
				throws IOException, InterruptedException {

			// Get file name informations
			FileSplit splitInfo = (FileSplit) context.getInputSplit();
			String[] filePath = splitInfo.getPath().toString().split("/");
			String fileDir = filePath[filePath.length - 2];

			// Extract the year of the line depending from which file it comes
			String year = "";
			if(fileDir.equals(articlesDir)) {
				year = fileDir;
			} else if(fileDir.equals(yearsDir)) {
				String fileName = splitInfo.getPath().getName();
				year = fileName.replaceAll("-r-[0-9]+", "");
			}
			
			// Outputs (year1:year2, word) depending from which file the line comes
			String[] tokens = line.toString().split("\\s+");
			if (tokens.length == 2) {
				String word = tokens[0];
				if (fileDir.equals(articlesDir)) {
					for (int i = firstYear; i <= lastYear; i++) {
						context.write(new Text(year + ":" + i), new Text(word));
					}
				} else if (fileDir.equals(yearsDir)){
					context.write(new Text(articlesDir + ":" + year), new Text(word));
				}
			}

		}
	}

	/**
	 * Reducer: returns the distance for each pair of years by looking at common words.
	 * 
	 * @author Cynthia, Farah
	 * 
	 */
	private static class CDistanceReducer extends
			Reducer<Text, Text, Text, DoubleWritable> {

		private HashMap<Integer, Integer> yearCardinals = null;
		double distance = 3000;

		/**
		 * Read the file containing the cardinalities of each year
		 */
		@Override
		public void setup(Context context) throws IOException {
			Path pt = new Path(
					"/projects/linguistic-shift/stats/YearCardinality");
			FileSystem hdfs = pt.getFileSystem(context.getConfiguration());
			if (hdfs.isFile(pt)) {
				distance = 2000;
				FSDataInputStream fis = hdfs.open(pt);
				BufferedReader br = new BufferedReader(new InputStreamReader(
						fis));
				String line = br.readLine();
				yearCardinals = new HashMap<Integer, Integer>();
				int year;
				int cardinals;

				while (line != null) {
					String[] elements = line.toString().split("\\s+");
					year = Integer.parseInt(elements[0]);
					cardinals = Integer.parseInt(elements[1]);
					yearCardinals.put(year, cardinals);

					line = br.readLine();
				}
				br.close();
				fis.close();
			}

		}

		/**
		 * The function deduce the number of common words by looking at words that appear twice in the input values.
		 * It computes the distance between two years using their cardinalities and their common words.
		 */
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Iterator<Text> valuesIt = values.iterator();
			HashSet<String> valuesSet = new HashSet<String>();
			int valuesSize = 0;

			// Count the number of elements and common words
			while (valuesIt.hasNext()) {
				String val = valuesIt.next().toString();
				valuesSize++;
				valuesSet.add(val);
			}
			double numCommonWords = valuesSize - valuesSet.size();

			// Compute the distance with the common words and the cardinality of each year
			String[] years = key.toString().split(":");
			if (yearCardinals != null) {
				int cardinal1 = yearCardinals
						.get(Integer.parseInt(years[0]));
				int cardinal2 = yearCardinals
						.get(Integer.parseInt(years[1]));
				distance = 1 - (2 * numCommonWords / (cardinal1 + cardinal2));
			}

			context.write(key, new DoubleWritable(distance));

		}

		@Override
		public void cleanup(Context context) {

		}
	}

	/**
	 * 
	 * @param args
	 *            arguments
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		
		String input1 = args[0];
		String input2 = args[1];

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "DistanceComputation");
		job.setJarByClass(Distance1Articles.class);
		job.setNumReduceTasks(NUM_REDUCERS);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setMapperClass(CDistanceMapper.class);
		job.setReducerClass(CDistanceReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		job.getConfiguration().set("inputDirArticles", input1);
		job.getConfiguration().set("inputDirTotalYears", input2);

		FileInputFormat.setInputDirRecursive(job, true);

		FileInputFormat.addInputPaths(job, input1+","+input2);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		MultipleOutputs.addNamedOutput(job, "Metric1", TextOutputFormat.class,
				Text.class, DoubleWritable.class);

		boolean done = job.waitForCompletion(true);

		if (done) {
			System.exit(0);
		} else {
			System.exit(1);
		}
	}

}