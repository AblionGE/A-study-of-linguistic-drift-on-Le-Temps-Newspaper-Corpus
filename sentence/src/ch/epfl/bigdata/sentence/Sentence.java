package ch.epfl.bigdata.sentence;

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
 * @author gbrechbu
 *
 */
public class Sentence {
	
	private static final int NUM_REDUCERS = 25;
	
	/**
	 * A simple mapper. Takes a full article and returns a <key/value> pair with key = "{year}word" and value = 1
	 * @author gbrechbu
	 *
	 */
	private static class NgramMapper extends  Mapper<IntWritable, Text, Text, IntWritable> {
		
		private String preProcess(String s) {
			String toRet = s.replaceAll("[^a-zA-ZÀÂÄÈÉÊËÎÏÔŒÙÛÜŸàâäèêéëîïôœùûüÿÇç.?!]", " ").toLowerCase();
			toRet = toRet.replaceAll("M.", "M");
			toRet = toRet.replaceAll("Mr.", "Mr");
			toRet = toRet.replaceAll("Mme.", "Mme");
			toRet = toRet.replaceAll("Mlle.", "Mlle");
			toRet = toRet.replaceAll("MM.", "MM");
			toRet = toRet.replaceAll("Dr.", "Dr");
			toRet = toRet.replaceAll("[A-Z]{1}\\.", "A");
			return toRet;
		}
		
		private static final IntWritable ONE = new IntWritable(1);
		
		private Text gram = new Text();
		private int sentenceSize;
		
		@Override
		public void map(IntWritable key, Text article, Context context) throws IOException, 
			InterruptedException {
			Configuration conf = context.getConfiguration();
			String separator = conf.get("separator", "\\s+");
			
			String stringArticle = article.toString();
			String tempArticle = preProcess(stringArticle).trim();
 			String[] splittedArticle = tempArticle.split("[.?!]");
 			
 			String year = String.valueOf(key.get()); 
 
 			for (int i = 0; i < splittedArticle.length; i++) {
 				String sentence = splittedArticle[i];
				String[] words = sentence.split("\\s+");
				sentenceSize = words.length;
				gram.set(year + "//" + Integer.toString(sentenceSize));
				context.write(gram, ONE);
			}
		}
	}
	
	/**.
	 * Combiner for the ngrams. Simply sums the values.
	 * @author gbrechbu
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
	 * @author gbrechbu
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
			String sentenceLength = parts[1];
			mout.write("Output", sentenceLength, new IntWritable(sum), year);
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
	 * 
	 * @param args arguments
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	public static void main(String[] args) throws IOException, ClassNotFoundException, 
			InterruptedException {
		Configuration conf = new Configuration();
		String[] userArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		Job job = Job.getInstance(conf, "Sentence Length");
		job.setJarByClass(Sentence.class);
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
