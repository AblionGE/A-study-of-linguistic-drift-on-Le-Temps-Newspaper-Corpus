package ch.epfl.bigdata.ngram;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Main class which contains the mapper class, the reducer class and the main function.
 * @author Gil Brechbühler and Malik Bougacha
 *
 */
public class Ngram {
	
	private static final int NUM_REDUCERS = 25;
	
	/**
	 * A simple mapper. Takes a full article and returns a <key/value> pair with key = "year//word" and value = 1
	 * @author Gil Brechbühler and Malik Bougacha
	 *
	 */
	private static class NgramMapper extends  Mapper<IntWritable, Text, Text, IntWritable> {
		
		/**
		 * Replaces everything that is not a letter (with accent or not) by a space.
		 */
		private String preProcess(String s) {
			return s.replaceAll("[^a-zA-ZÀÂÄÈÉÊËÎÏÔŒÙÛÜŸàâäèêéëîïôœùûüÿÇç]", " ").toLowerCase();
		}
		
		private static final IntWritable ONE = new IntWritable(1);
		
		private Text gram = new Text();
		private int ngramSize;
		private String ngramSeparator = ",";
		
		private String concat(Collection<String> stringCollection) {
			StringBuilder concatenator = new StringBuilder();
			for (Iterator<String> iterator = stringCollection.iterator(); iterator
					.hasNext();) {
				String string = (String) iterator.next();
				concatenator.append(string);
				if (iterator.hasNext()) {
					concatenator.append(ngramSeparator);
				}
			}
			return concatenator.toString();
		}
		
		@Override
		public void map(IntWritable key, Text article, Context context) throws IOException, 
			InterruptedException {
			Configuration conf = context.getConfiguration();
			// The ngramSize to compute defaults to 2 if no parameter is given to the job.
			ngramSize = conf.getInt("ngramSize", 2);
			String separator = conf.get("separator", "\\s+");
			ngramSeparator = conf.get("ngramSeparator", ",");
			
			String stringArticle = article.toString();
			String tempArticle = preProcess(stringArticle).trim();
 			String[] splittedArticle = tempArticle.split(separator);
 			
 			List<String> list = new ArrayList<String>(Arrays.asList(splittedArticle));
 			list.removeAll(Arrays.asList(""));
 			splittedArticle = list.toArray(new String[0]);
 			
 			/*
 			 * The Dequeue is first filled with n-1 1-grams. Then at each step, a new 1-gram is added at the tail of
 			 * the queue. The ngram is computed (all elements in queue) and the first element (head) of the queue
 			 * is removed, this is a simple optimization for the ngrams computation.
 			 */
			Deque<String> currentNgram = new ArrayDeque<>();
			int counter = 0;
			
			
			//article is too small
			if (splittedArticle.length < ngramSize) {
			    return;
			}
			
			for (; counter < ngramSize - 1; counter++) {
				 currentNgram.addLast(splittedArticle[counter]);
			}

			String year = String.valueOf(key.get());

			for (; counter < splittedArticle.length; counter++) {
			    currentNgram.addLast(splittedArticle[counter]);
				gram.set(year + "//" + concat(currentNgram));
				context.write(gram, ONE);
				currentNgram.removeFirst();
			}
		}
	}
	
	/**.
	 * Combiner for the ngrams. Simply sums the values.
	 * @author Gil Brechbühler and Malik Bougacha
	 *
	 */
	private static class NgramCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {		
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) 
				throws IOException, InterruptedException {
			int sum = 0;
			Iterator<IntWritable> valuesIt = values.iterator();
			
			while (valuesIt.hasNext()) {
				IntWritable value = (IntWritable) valuesIt.next();
				sum += value.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}
	
	/**.
	 * Reducer for the ngrams.
	 * @author Gil Brechbühler and Malik Bougacha
	 *
	 */
	private static class NgramReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		
		private MultipleOutputs<Text, IntWritable> mout;
		
		@Override
		public void setup(Context context) {
			 mout = new MultipleOutputs<Text, IntWritable>(context);
		}
		
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) 
				throws IOException, InterruptedException {
			int sum = 0;
			Iterator<IntWritable> valuesIt = values.iterator();
			
			while (valuesIt.hasNext()) {
				IntWritable value = (IntWritable) valuesIt.next();
				sum += value.get();
			}
			String[] parts = key.toString().split("//", 2);
			String year = parts[0];
			String ngram = parts[1];
			mout.write("Output", ngram, new IntWritable(sum), year);
		}
		
		@Override
		public void cleanup(Context context) {
			try {
				mout.close();
			} catch (IOException | InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * Setup and running of the job.
	 * @param args arguments
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	public static void main(String[] args) throws IOException, ClassNotFoundException, 
			InterruptedException {
		Configuration conf = new Configuration();
		String[] userArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		Job job = Job.getInstance(conf, "Ngram");
		job.setJarByClass(Ngram.class);
		job.setNumReduceTasks(NUM_REDUCERS);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
        
		job.setMapperClass(NgramMapper.class);
		job.setReducerClass(NgramReducer.class);
		job.setCombinerClass(NgramCombiner.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Integer.class);
		
		job.setInputFormatClass(XmlFileInputFormat.class);
		
		FileInputFormat.setInputDirRecursive(job, true);
		
		XmlFileInputFormat.addInputPath(job, new Path(userArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(userArgs[1]));
		MultipleOutputs.addNamedOutput(job, "Output", TextOutputFormat.class, Text.class, IntWritable.class);
		
		boolean done = job.waitForCompletion(true);
		
		if (done) {
			System.exit(0);
		} else {
			System.exit(1);
		}
	}
}
