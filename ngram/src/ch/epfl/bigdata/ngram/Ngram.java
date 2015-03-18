package ch.epfl.bigdata.ngram;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
 * Test.
 * @author gbrechbu
 *
 */
public class Ngram {
	
	/**
	 * A simple mapper. Takes a full article and returns a <key/value> pair with value = 1 and key = "{year}word"
	 * @author gbrechbu
	 *
	 */
	private static class NgramMapper extends  Mapper<IntWritable, Text, Text, IntWritable> {
		
		private static final IntWritable ONE = new IntWritable(1);
		
		private Text gram = new Text();
		private int ngramSize = 1;
		private String ngramSeparator;
		
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
			
			String stringArticle = article.toString();
			String[] words = stringArticle.split("\\s+");
			//StringTokenizer wordIterator = new StringTokenizer(stringArticle);
			Deque<String> currentNgram = new ArrayDeque<>();
			
			//initialize the Deque
			for (int i = 0; i < ngramSize; i++) {
				//String nextToken = wordIterator.nextToken();
				//currentNgram.add(nextToken);
			}
			
			for (int i = 0; i < words.length; i++) {
				String word = words[i];
				String year = "{" + String.valueOf(key.get()) + "}";
				String toReturn = year + word;
				gram.set(toReturn);
				context.write(gram, ONE);
			}
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
			String year = new String();
			
			while (valuesIt.hasNext()) {
				IntWritable value = (IntWritable) valuesIt.next();
				sum += value.get();
			}
			Pattern pattern = Pattern.compile("(\\{\\d*\\})");
			Matcher matcher = pattern.matcher(key.toString());
			if (matcher.find()) {
				System.out.println("foud !");
				year = matcher.group(1);
			}
			Text finalKey = new Text(key.toString().replace(year, ""));
			year = year.replace("{", "");
			year = year.replace("}", "");
			Path outPath = FileOutputFormat.getOutputPath(context);
			//Path finalPath = new Path(outPath.toString() + "/" + year.toString());
			mout.write("Output", finalKey, new IntWritable(sum), year);
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
		//job.setNumReduceTasks(2);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
        
		job.setMapperClass(NgramMapper.class);
		job.setReducerClass(NgramReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Integer.class);
		
		job.setInputFormatClass(XmlFileInputFormat.class);
		
		FileInputFormat.setInputDirRecursive(job, true);
		
		XmlFileInputFormat.addInputPath(job, new Path(userArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(userArgs[1]));
		MultipleOutputs.addNamedOutput(job, "Output", TextOutputFormat.class, Text.class, IntWritable.class);
		
		boolean done = job.waitForCompletion(true);
		
		if (done) {
			/*Path path = FileOutputFormat.getOutputPath(job);
			FileSystem fs = path.getFileSystem(job.getConfiguration());
			fs.deleteOnExit(new Path(path.toString() + "/_SUCCESS"));*/
			System.exit(0);
		} else {
			System.exit(1);
		}
	}
}
