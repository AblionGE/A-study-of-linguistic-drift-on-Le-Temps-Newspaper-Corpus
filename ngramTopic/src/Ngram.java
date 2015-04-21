

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

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
public class Ngram {
	
	private static final int NUM_REDUCERS = 25;
	
	/**
	 * A simple mapper. Takes a full article and returns a <key/value> pair with key = "{year}word" and value = 1
	 * @author gbrechbu
	 *
	 */
	private static class NgramMapper extends  Mapper<IntWritable, Text, Text, IntWritable> {
		
		private String preProcess(String s) {
			return s.replaceAll("[^a-zA-ZÃ€Ã‚Ã„ÃˆÃ‰ÃŠÃ‹ÃŽÃ�Ã”Å’Ã™Ã›ÃœÅ¸Ã Ã¢Ã¤Ã¨ÃªÃ©Ã«Ã®Ã¯Ã´Å“Ã¹Ã»Ã¼Ã¿Ã‡Ã§]", " ").toLowerCase();
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
			ngramSize = conf.getInt("ngramSize", 1);
			String separator = conf.get("separator", "\\s+");
			ngramSeparator = conf.get("ngramSeparator", ",");
			Random rand = new Random();
			int articleID = rand.nextInt(Integer.MAX_VALUE);
			String stringArticle = article.toString();
			String tempArticle = preProcess(stringArticle).trim();
 			String[] splittedArticle = tempArticle.split(separator);
			
 			List<String> list = new ArrayList<String>(Arrays.asList(splittedArticle));
 			list.removeAll(Arrays.asList(""));
 			splittedArticle = list.toArray(new String[0]);
 			
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
				gram.set(year +"//"+ articleID+"//" + concat(currentNgram));
				context.write(gram, ONE);
				currentNgram.removeFirst();
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
	private static class NgramReducer extends Reducer<Text, IntWritable, Text, Text> {
		
		private MultipleOutputs<Text, Text> mout;
		
		@Override
		public void setup(Context context) {
			 mout = new MultipleOutputs<Text, Text>(context);
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
			String[] parts = key.toString().split("//", 3);
			String year = parts[0];
			String articleID = parts[1];
			String ngram = parts[2];
			
			mout.write("Output", ngram, sum+"\t"+articleID, year);
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
		MultipleOutputs.addNamedOutput(job, "Output", TextOutputFormat.class, Text.class, Text.class);
		
		boolean done = job.waitForCompletion(true);
		
		if (done) {
			System.exit(0);
		} else {
			System.exit(1);
		}
	}
}
