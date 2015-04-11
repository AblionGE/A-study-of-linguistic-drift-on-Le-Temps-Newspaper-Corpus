

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class YearCardinality {

	public static class CardinalityMapper extends Mapper<LongWritable, Text, Text, Text> {
		private Text word = new Text();
		private Text year = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			FileSplit splitInfo = (FileSplit) context.getInputSplit();
			String fileName = splitInfo.getPath().getName();
			year.set(fileName.replaceAll("-r-[0-9]+", ""));

			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			String s;
			while (tokenizer.hasMoreTokens()) {
				s = tokenizer.nextToken();
				if(!s.matches("[0-9]+")) {
					word.set(s);
					context.write(year, word);
				}
			}
		}
	} 

	public static class CardinalityReducer extends Reducer<Text, Text, Text, IntWritable> {

		private MultipleOutputs<Text, IntWritable> mos;
		private IntWritable cardinality = new IntWritable();

		public void setup(Context context) {
			mos = new MultipleOutputs<Text,IntWritable>(context);
		}

		public void reduce(Text key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException {

			Iterator<Text> valuesIt = values.iterator();
			
			int size = 0;
			while (valuesIt.hasNext()) {
				valuesIt.next();
				size++;
			}

			cardinality.set(size);
			mos.write("Output", key, cardinality, "YearCardinality");
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

		Job job = Job.getInstance(conf, "YearCardinality");
		job.setJarByClass(YearCardinality.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(CardinalityMapper.class);
		job.setReducerClass(CardinalityReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		MultipleOutputs.addNamedOutput(job, "Output", TextOutputFormat.class, Text.class, IntWritable.class);

		job.waitForCompletion(true);
	}

}
