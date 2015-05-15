import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * 
 * @author jweber & fbouassid
 * 
 */
public class ComputeArticleTopic {

	private static final int NUM_REDUCERS = 50;

	/**
	 * INPUT: It takes in input : articleID,topicNumber or
	 * aritcleID,CompactBuffer((word,Occurence),(word2,Occurence),...) OUPTPUT:
	 * 1991//937076246917975040 reglements,1 trois,1
	 * 
	 * @author jweber & fbouassid
	 * 
	 */
	private static class ComputeArticleTopicMapper extends
			Mapper<LongWritable, Text, Text, Text> {

		@Override
		public void map(LongWritable key, Text nonParsedData, Context context)
				throws IOException, InterruptedException {

			// the array can be two different thing.
			// Either {articleID,word1,occurence,word2,occurence,...}
			// or {articleID,topicNumber}
			String[] inputMapper = null;
			String stringArticle = nonParsedData.toString();

			// System.out.println("We have receive "+stringArticle);
			if (stringArticle.contains("CompactBuffer")) {
				inputMapper = stringArticle.split(",CompactBuffer|, |\\(|\\)",
						-1);
				// remove a "," at the end of the year//articleID,
				String articleID = inputMapper[1];
				// We start at 1 because we don't care about the first item

				for (int i = 2; i < inputMapper.length; i++) {
					if (!inputMapper[i].isEmpty()) {
						context.write(new Text(articleID), new Text(
								inputMapper[i]));
					}
				}

			} else {
				inputMapper = stringArticle.split("\\(|\\)|,", -1);
				if (!inputMapper[1].isEmpty() && !inputMapper[2].isEmpty()) {
					context.write(new Text(inputMapper[1]), new Text(
							inputMapper[2]));
				}
			}
		}
	}

	/**
	 * Reducer for the Article by topic. Input: Key: ArticleID, Value:
	 * word,occurence<TAB>word,occurence<TAB>topicNumber
	 * 
	 * Output : Ky value pairs of TopicNumber//year//word occurence
	 * 
	 * @author jweber & fbouassid
	 * 
	 */
	private static class ComputeArticleTopicReducer extends
			Reducer<Text, Text, Text, IntWritable> {

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			HashSet<String> wordInArticle = new HashSet<String>();
			String topicNumber = null;

			String year = key.toString().substring(0, 4);
			for (Text wordOccText : values) {
				String wordOcc = wordOccText.toString();
				if (wordOcc.contains(",") && !wordOcc.isEmpty()) {
					wordInArticle.add(year + "//" + wordOcc);
				} else {
					topicNumber = wordOcc;
				}
			}
			if (!wordInArticle.toString().isEmpty() && topicNumber != null) {
				for (String gram : wordInArticle) {
					String[] tokens = gram.split(","); // token[0]: year//word
														// token[1]: Occ
					context.write(new Text(topicNumber + "//" + tokens[0]),
							new IntWritable(Integer.parseInt(tokens[1])));
				}

			}
		}
	}

	/**
	 * OUTPUT: key value pairs topicNumber, year//word,occurence
	 * 
	 * @author jweber & fbouassid
	 * 
	 */
	private static class MergeArticleByTopicMapper extends
			Mapper<Text, Text, Text, Text> {

		@Override
		public void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] tokens = key.toString().split("//");

			context.write(new Text(tokens[0]), new Text(tokens[1] + "//"
					+ tokens[2] + "," + value));

		}
	}

	/**
	 * Sum the occurences of a word by year by topic
	 * 
	 * @author fbouassid & jweber
	 * 
	 */

	public static class wordCountReduce extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			int sum = 0;
			for (Text occ : values) {
				sum += new Integer(occ.toString());
			}
			context.write(key, new Text(String.valueOf(sum)));
		}

	}

	/**
	 * Reducer for the Article by topic. Input: Key: ArticleID, Value:
	 * word,occurence<TAB>word,occurence<TAB>topicNumber
	 * 
	 * @author jweber & fbouassid
	 */
	private static class MergeArticleByTopicReducer extends
			Reducer<Text, Text, Text, Text> {
		private MultipleOutputs<Text, Text> mout;

		@Override
		public void setup(Context context) {
			mout = new MultipleOutputs<Text, Text>(context);
		}

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			for (Text yearWordOcc : values) {
				// Get the year, word and occurence
				String[] wordOccYear = yearWordOcc.toString().split(
						"\\[|\\]|, ");

				for (int i = 0; i < wordOccYear.length; i++) {
					if (!wordOccYear[i].isEmpty()) {
						String year = wordOccYear[i].split("//")[0];
						mout.write(
								"Output",
								new Text(wordOccYear[i].split("//")[1]
										.split(",")[0]),
								new Text(wordOccYear[i].split("//")[1]
										.split(",")[1]), "topic" + key + "/"
										+ year);

					}
				}

			}

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
	 * Launcher
	 * 
	 * @param args
	 *            arguments
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 * @author jweber & fbouassid
	 */
	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		// String[] userArgs = new GenericOptionsParser(conf, args)
		// .getRemainingArgs();
		Job job = Job.getInstance(conf, "Compute Article Topic");
		job.setJarByClass(ComputeArticleTopic.class);
		job.setNumReduceTasks(NUM_REDUCERS);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setMapperClass(ComputeArticleTopicMapper.class);
		job.setReducerClass(ComputeArticleTopicReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2] + "-Tmp"));
		job.waitForCompletion(true);

		Job job2 = Job.getInstance(conf, "Merge 1-gram occurences");
		job2.setJarByClass(ComputeArticleTopic.class);
		job2.setNumReduceTasks(NUM_REDUCERS);

		job2.setInputFormatClass(KeyValueTextInputFormat.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);

		job2.setReducerClass(wordCountReduce.class);

		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job2, new Path(args[2] + "-Tmp"));
		FileOutputFormat.setOutputPath(job2, new Path(args[2] + "-Tmp2"));

		job2.waitForCompletion(true);

		Job job3 = Job.getInstance(conf, "Compute Article Topic 2ndPart");
		job3.setJarByClass(ComputeArticleTopic.class);
		job3.setNumReduceTasks(NUM_REDUCERS);

		job3.setInputFormatClass(KeyValueTextInputFormat.class);
		job3.setMapOutputKeyClass(Text.class);
		job3.setMapOutputValueClass(Text.class);
		job3.setMapperClass(MergeArticleByTopicMapper.class);
		job3.setReducerClass(MergeArticleByTopicReducer.class);

		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job3, new Path(args[2] + "-Tmp2"));
		FileOutputFormat.setOutputPath(job3, new Path(args[2]));

		MultipleOutputs.addNamedOutput(job3, "Output", TextOutputFormat.class,
				Text.class, Text.class);

		job3.waitForCompletion(true);

	}
}