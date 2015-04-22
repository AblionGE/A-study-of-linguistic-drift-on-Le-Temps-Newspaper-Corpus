package ch.epfl.bigdata;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


/**
 * Computes the number of occurences of words over all years
 * @author Cynthia
 *
 */
public class WordFrequencyOverAllYears {
	
	/**
	 * Mapper: takes a directory with 1-grams files as input and outputs a key/value pair of (word, frequency) for each word and each year
	 * @author cynthia
	 *
	 */
	public static class FrequencyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		private Text word = new Text();
		private IntWritable frequency = new IntWritable();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String[] tokens = value.toString().split("\\s+");
			if (tokens.length == 2) {
				word.set(tokens[0]);
				frequency.set(Integer.parseInt(tokens[1]));
				context.write(word, frequency);
			}
		}
	} 

	/**
	 * Reducer: outputs a word as key and the number of times it appears over all years as value
	 * @author cynthia
	 *
	 */
	public static class FrequencyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		private MultipleOutputs<Text, IntWritable> mos;
		private IntWritable frequency = new IntWritable();

		public void setup(Context context) {
			mos = new MultipleOutputs<Text,IntWritable>(context);
		}

		public void reduce(Text key, Iterable<IntWritable> values, Context context) 
				throws IOException, InterruptedException {

			Iterator<IntWritable> valuesIt = values.iterator();

			int sum = 0;
			while (valuesIt.hasNext()) {
				sum += valuesIt.next().get();
			}

			frequency.set(sum);
			mos.write("Output", key, frequency, "WordFrequencyOverAllYears");
		}

		public void cleanup(Context context) {
			try {
				mos.close();
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "WordFrequencyOverAllYears");
		job.setJarByClass(WordFrequencyOverAllYears.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(FrequencyMapper.class);
		job.setReducerClass(FrequencyReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		MultipleOutputs.addNamedOutput(job, "Output", TextOutputFormat.class, Text.class, IntWritable.class);

		job.waitForCompletion(true);
	}

}
