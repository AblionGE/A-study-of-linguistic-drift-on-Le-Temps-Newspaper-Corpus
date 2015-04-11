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
 * 
 * @author Cynthia, Farah
 * 
 */

public class DistanceComputation {


	/**
	 * Mapper to compute distance: takes a directory with all 1-gram files as
	 * input. For one year y1 and for each words in y1, it returns a tuple for
	 * each y2 in [1840,1998]: (y1:y2, word).
	 * 
	 * @author Cynthia, Farah
	 * 
	 */
	private static class CDistanceMapper extends
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
				for (int i = firstYear; i <= lastYear; i++) {
					if (Integer.parseInt(year) < i) {
						context.write(new Text(year + ":" + i), new Text(
								tokens[1]));
					} else {
						context.write(new Text(i + ":" + year), new Text(
								tokens[1]));
					}

				}
			}

		}
	}

	/**
	 * Reducer to compute distance: returns the distance for each pair of years.
	 * 
	 * @author Cynthia, Farah
	 * 
	 */
	private static class CDistanceReducer extends
			Reducer<Text, Text, Text, DoubleWritable> {

		private HashMap<Integer, Integer> yearOccurences;
		double distance = 3000;

		@Override
		public void setup(Context context) throws IOException {
			Path pt = new Path(
					"/projects/linguistic-shift/tfidf/1-grams-TotOccurenceYear/YearOccurences");
			FileSystem hdfs = pt.getFileSystem(context.getConfiguration());
			if (hdfs.isFile(pt)) {
				distance = 2000;
				FSDataInputStream fis = hdfs.open(pt);
				BufferedReader br = new BufferedReader(new InputStreamReader(
						fis));
				String line = br.readLine();
				yearOccurences = new HashMap<Integer, Integer>();
				int year;
				int occurences;

				while (line != null) {
					String[] elements = line.toString().split("\\s+");
					year = Integer.parseInt(elements[0]);
					occurences = Integer.parseInt(elements[1]);
					yearOccurences.put(year, occurences);

					line = br.readLine();
				}
				br.close();
				fis.close();
			}

		}

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Iterator<Text> valuesIt = values.iterator();
			List<String> valuesList = new ArrayList<String>();

			while (valuesIt.hasNext()) {
				String val = valuesIt.next().toString();
				valuesList.add(val);
			}

			Set valuesSet = new HashSet(valuesList);
			double numCommonWords = valuesList.size() - valuesSet.size();

			String[] years = key.toString().split(":");
//			double distance = 3000; // si ça ne marche pas
			if (yearOccurences != null) {
				int occurences1 = yearOccurences
						.get(Integer.parseInt(years[0]));
				int occurences2 = yearOccurences
						.get(Integer.parseInt(years[1]));
				distance = 1 - (2 * numCommonWords / (occurences1 + occurences2));
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

		Job job = Job.getInstance(conf, "DistanceComputation");
		job.setJarByClass(DistanceComputation.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setMapperClass(CDistanceMapper.class);
		job.setReducerClass(CDistanceReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		FileInputFormat.setInputDirRecursive(job, true);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		MultipleOutputs.addNamedOutput(job, "Metric1", TextOutputFormat.class,
				Text.class, DoubleWritable.class);

		boolean done = job.waitForCompletion(true);

		if (done) {
			System.exit(0);
		} else {
			System.exit(1);
		}
	}

}
