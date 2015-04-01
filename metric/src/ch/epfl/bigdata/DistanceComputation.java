package ch.epfl.bigdata;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * 
 * @author Cynthia, Farah
 * 
 */

public class DistanceComputation {

	/**
	 * A simple mapper. Takes a full article and returns a <key/value> pair with
	 * value = 1 and key = "{year}word"
	 * 
	 * @author gbrechbu
	 * 
	 */
	private static class CDistanceMapper extends
			Mapper<LongWritable, Text, Text, Text> {

		private static final IntWritable ONE = new IntWritable(1);
		private int firstYear = 1840;
		private int lastYear = 1998;

		@Override
		public void map(LongWritable key, Text line, Context context)
				throws IOException, InterruptedException {

			FileSplit splitInfo = (FileSplit) context.getInputSplit();
			String fileName = splitInfo.getPath().getName();
			String[] tokens = line.toString().split(" ");
			for (int i = firstYear; i <= lastYear; i++) {
				if(Integer.parseInt(fileName) < i){
					context.write(new Text(fileName+":"+i) , new Text(tokens[0]));
				} else { 
					context.write(new Text(i+":"+fileName) , new Text(tokens[0]));
				}
				
			}

		}
	}

	/**
	 * . Reducer for the ngrams.
	 * 
	 * @author Cynthia, Farah
	 * 
	 */
	private static class CDistanceReducer extends
			Reducer<Text, Text, Text, IntWritable> {

		private MultipleOutputs<Text, IntWritable> mout;

		@Override
		public void setup(Context context) {
			mout = new MultipleOutputs<Text, IntWritable>(context);
		}

		@Override
		public void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			Iterator<Text> valuesIt = values.iterator();
			
			int sum = 0;
			while (valuesIt.hasNext()) {
				valuesIt.next();
				sum++;
			}

			context.write(key, new IntWritable(sum));
		}

		@Override
		public void cleanup(Context context) {
			try {
				mout.close();
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
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
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "DistanceMetric");
		job.setJarByClass(DistanceComputation.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setMapperClass(CDistanceMapper.class);
		job.setReducerClass(CDistanceReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Integer.class);

		FileInputFormat.setInputDirRecursive(job, true);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		MultipleOutputs.addNamedOutput(job, "Metric1", TextOutputFormat.class,
				Text.class, IntWritable.class);

		boolean done = job.waitForCompletion(true);

		if (done) {
			System.exit(0);
		} else {
			System.exit(1);
		}
	}

}
