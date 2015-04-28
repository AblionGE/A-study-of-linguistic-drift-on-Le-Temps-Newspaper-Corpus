

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
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Main class which contains the mapper class, the reducer class and the main function.
 * We seperate by the year by article
 * @author jweber
 *
 */
public class WordArticle {
	
	private static final int NUM_REDUCERS = 25;
	
	/**
	 * A simple mapper.
	 * It takes in input : word <TAB> occurence <TAB> articleID
	 * @author jweber
	 *
	 */
	private static class WordArticleMapper extends  Mapper<LongWritable, Text, Text, Text> {
		
		@Override
		public void map(LongWritable key, Text article, Context context) throws IOException, 
			InterruptedException {
			Configuration conf = context.getConfiguration();
			String year = ((FileSplit) context.getInputSplit()).getPath().getName().toString();
			
			String stringArticle = article.toString();
			String[] element= stringArticle.split("\t");
			String word = element[0];
			String occurence = element[1];
			String articleID = element[2];
			
			context.write(new Text(year+"//"+articleID), new Text(word+"\t"+occurence));
		}
	}
	

	
	/**.
	 * Reducer for the ngrams by article.
	 * @author jweber
	 *
	 */
	private static class WordArticleReducer extends Reducer<Text, Text, Text, Text> {
		
		private MultipleOutputs<Text, Text> mout;
		
		@Override
		public void setup(Context context) {
			 mout = new MultipleOutputs<Text, Text>(context);
		}
		
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) 
				throws IOException, InterruptedException {
			String listWordOcc = "";
			Iterator<Text> valuesIt = values.iterator();
			while (valuesIt.hasNext()) {
				listWordOcc = listWordOcc + valuesIt.next().toString()+",";
				
			}
			String[] parts = key.toString().split("//", 2);
			String year = parts[0];
			String articleID = parts[1];
			mout.write("Output", new Text(year+"//"+articleID),new Text(listWordOcc) , year);
		}
		
		@Override
		public void cleanup(Context context) {
			try {
				mout.close();
			} catch (Exception e) {
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
	 * @author Jweber
	 */
	public static void main(String[] args) throws IOException, ClassNotFoundException, 
			InterruptedException {
		Configuration conf = new Configuration();
		String[] userArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		Job job = Job.getInstance(conf, "WordArticle");
		job.setJarByClass(WordArticle.class);
		job.setNumReduceTasks(NUM_REDUCERS);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
        
		job.setMapperClass(WordArticleMapper.class);
		job.setReducerClass(WordArticleReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    MultipleOutputs.addNamedOutput(job, "Output", TextOutputFormat.class, Text.class, Text.class);
		boolean done = job.waitForCompletion(true);
		
		if (done) {
			System.exit(0);
		} else {
			System.exit(1);
		}
	}
}
