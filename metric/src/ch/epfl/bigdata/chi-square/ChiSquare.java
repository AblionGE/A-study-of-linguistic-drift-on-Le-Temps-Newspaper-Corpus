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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.util.List;


/**
 * Computes Chi-Square distance
 * @author Cynthia
 *
 */
public class ChiSquare {
	private static final int NUM_REDUCERS = 25;

	/**
	 * Mapper to compute chi-square distance: takes a directory with all 1-gram files as
	 * input. For one year y1 and for each words in y1, it returns a tuple for
	 * each y2 in [1840,1998]: (y1:y2, y1/word/frequency).
	 * 
	 * @author Cynthia
	 * 
	 */
	private static class CSMapper extends
	Mapper<LongWritable, Text, Text, Text> {

		private final int firstYear = 1840;
		private final int lastYear = 1998;

		@Override
		public void map(LongWritable key, Text line, Context context)
				throws IOException, InterruptedException {

			FileSplit splitInfo = (FileSplit) context.getInputSplit();
			String fileName = splitInfo.getPath().getName();
			String year = fileName.replaceAll("-r-[0-9]+", "");
			String[] tokens = line.toString().split("\\s+");

			if (tokens.length == 2) {
				String word = tokens[0];
				String frequency = tokens[1];
				for (int i = firstYear; i <= lastYear; i++) {
					if (Integer.parseInt(year) <= i) {
						context.write(new Text(year + ":" + i), new Text(
								year+"/"+word+"/"+frequency));
					}
					if (Integer.parseInt(year) >= i) {
						context.write(new Text(i + ":" + year), new Text(
								year+"/"+word+"/"+frequency));
					}

				}
			}

		}
	}

	/**
	 * Reducer to compute chi-square distance: returns the distance for each pair of years.
	 * 
	 * @author Cynthia
	 * 
	 */
	private static class CSReducer extends
	Reducer<Text, Text, Text, DoubleWritable> {

		private HashMap<Integer, Integer> yearOccurences = null;
		private HashMap<String, Integer> wordOccurences = null;
		double distance = 3000;

		@Override
		public void setup(Context context) throws IOException {
			Path pt = new Path(
					"/projects/linguistic-shift/stats/1-grams-TotOccurenceYear/YearOccurences");
			FileSystem hdfs = pt.getFileSystem(context.getConfiguration());
			if (hdfs.isFile(pt)) {
				FSDataInputStream fis = hdfs.open(pt);
				BufferedReader br = new BufferedReader(new InputStreamReader(
						fis));
				String line = br.readLine();
				yearOccurences = new HashMap<Integer, Integer>();
				int year;
				int wordCount;

				while (line != null) {
					String[] elements = line.toString().split("\\s+");
					year = Integer.parseInt(elements[0]);
					wordCount = Integer.parseInt(elements[1]);
					yearOccurences.put(year, wordCount);

					line = br.readLine();
				}
				br.close();
				fis.close();
			}
			
			pt = new Path(
					"/projects/linguistic-shift/distances/WordOccurenceOverAllYears");
			hdfs = pt.getFileSystem(context.getConfiguration());
			if (hdfs.isFile(pt)) {
				distance = 2000;
				FSDataInputStream fis = hdfs.open(pt);
				BufferedReader br = new BufferedReader(new InputStreamReader(
						fis));
				String line = br.readLine();
				wordOccurences = new HashMap<String, Integer>();
				String word;
				int occurences;

				while (line != null) {
					String[] elements = line.toString().split("\\s+");
					word = elements[0];
					occurences = Integer.parseInt(elements[1]);
					wordOccurences.put(word, occurences);

					line = br.readLine();
				}
				br.close();
				fis.close();
			}

		}

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			HashMap<String,Integer> freqYear1 = new HashMap<String, Integer>();
			HashMap<String,Integer> freqYear2 = new HashMap<String, Integer>();
			HashSet<String> words = new HashSet<String>();

			Iterator<Text> valuesIt = values.iterator();
			String[] years = key.toString().split(":");
			String year1 = years[0];
			String year2 = years[1];

			while (valuesIt.hasNext()) {
				String[] val = valuesIt.next().toString().split("/");
				if(val[0].equals(year1)) {
					freqYear1.put(val[1], Integer.parseInt(val[2]));
				} else if(val[0].equals(year2)) {
					freqYear2.put(val[1], Integer.parseInt(val[2]));
				}
				words.add(val[1]);
			}


			if (yearOccurences != null) {
				double wordCount1 = (double)yearOccurences
						.get(Integer.parseInt(year1));
				double wordCount2 = yearOccurences
						.get(Integer.parseInt(year2));

				Iterator<String> wordsIt = words.iterator();
				distance = 0;
				int freq1, freq2;
				while (wordsIt.hasNext()) {
					String word = wordsIt.next();
					double wordOccurence = (double)wordOccurences.get(word);
					freq1 = 0;
					freq2 = 0;

					if(freqYear1.containsKey(word)) {
						freq1 = freqYear1.get(word);
					}
					if(freqYear2.containsKey(word)) {
						freq2 = freqYear2.get(word);

					}
					distance += Math.pow(freq1/wordCount1 - freq2/wordCount2, 2) / wordOccurence;
				}
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

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "ChiSquare");
		job.setJarByClass(ChiSquare.class);
		job.setNumReduceTasks(NUM_REDUCERS);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setMapperClass(CSMapper.class);
		job.setReducerClass(CSReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		FileInputFormat.setInputDirRecursive(job, true);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		MultipleOutputs.addNamedOutput(job, "ChiSquare", TextOutputFormat.class,
				Text.class, DoubleWritable.class);

		boolean done = job.waitForCompletion(true);

		if (done) {
			System.exit(0);
		} else {
			System.exit(1);
		}
	}
}
