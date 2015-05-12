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
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.util.List;

/**
 * Computes a cosine based distance between years.
 * 
 * @author Cynthia
 * 
 */
public class Cosine {
    private static final int NUM_REDUCERS = 50;

    /**
     * Mapper to compute cosine similarity: takes a directory with all 1-gram
     * files as input. For one year y1 and for each words in y1, it returns a
     * tuple for each y2 in [1840,1998]: (y1:y2, y1/word/frequency).
     * 
     * @author Cynthia
     * 
     */
    private static class CosMapper extends
	    Mapper<LongWritable, Text, Text, Text> {

	private final int firstYear = 1840;
	private final int lastYear = 1998;
	private Text years = new Text();
	private Text values = new Text();

	/**
	 * The function extracts the name of the file from which the line comes
	 * and determine the year corresponding to it. Then a key-value pair is
	 * returned for each year in the corpus. It ensures that for every
	 * output key y1:y2, y1 <= y2.
	 */
	@Override
	public void map(LongWritable key, Text line, Context context)
		throws IOException, InterruptedException {

	    // Get file name informations
	    FileSplit splitInfo = (FileSplit) context.getInputSplit();
	    String fileName = splitInfo.getPath().getName();
	    String year = fileName.replaceAll("-r-[0-9]+", "");
	    String[] tokens = line.toString().split("\\s+");

	    // Order the elements of the key and output it with the word coming
	    // from the line
	    if (tokens.length == 2) {
		String word = tokens[0];
		String frequency = tokens[1];
		values.set(year + "/" + word + "/" + frequency);
		for (int i = firstYear; i <= lastYear; i++) {
		    if (Integer.parseInt(year) <= i) {
			years.set(year + ":" + i);
		    } else {
			years.set(i + ":" + year);
		    }
		    context.write(years, values);
		}
	    }

	}
    }

    /**
     * Reducer to compute distance 1-cos_similarity: returns the distance for
     * each pair of years.
     * 
     * @author Cynthia
     * 
     */
    private static class CosReducer extends
	    Reducer<Text, Text, Text, DoubleWritable> {

	private DoubleWritable distance = new DoubleWritable();

	/**
	 * Outputs the distance: 1-cos_similarity
	 */
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
		throws IOException, InterruptedException {

	    HashMap<String, Double> freqYear1 = new HashMap<String, Double>();
	    HashMap<String, Double> freqYear2 = new HashMap<String, Double>();
	    HashSet<String> words = new HashSet<String>();

	    // Store the frequencies for each words in both years in tables
	    // Store the distinct words in a HashSet
	    // Compute the norm for each year
	    Iterator<Text> valuesIt = values.iterator();
	    String[] years = key.toString().split(":");
	    String year1 = years[0];
	    String year2 = years[1];

	    double frequency = 0.0;
	    double norm1 = 0.0;
	    double norm2 = 0.0;
	    while (valuesIt.hasNext()) {
		String[] val = valuesIt.next().toString().split("/");
		words.add(val[1]);
		frequency = Double.parseDouble(val[2]);
		if (val[0].equals(year1)) {
		    freqYear1.put(val[1], frequency);
		    norm1 += Math.pow(frequency, 2);
		}
		if (val[0].equals(year2)) {
		    freqYear2.put(val[1], frequency);
		    norm2 += Math.pow(frequency, 2);
		}
	    }

	    // Computes the distance only if both years contain words
	    if (!freqYear1.isEmpty() && !freqYear2.isEmpty()) {
		double similarity = 0.0;
		double freq1, freq2;
		Iterator<String> wordsIt = words.iterator();
		while (wordsIt.hasNext()) {
		    freq1 = 0.0;
		    freq2 = 0.0;
		    String word = wordsIt.next();

		    // Cosine similarity:
		    if (freqYear1.containsKey(word)) {
			freq1 = freqYear1.get(word);
		    }
		    if (freqYear2.containsKey(word)) {
			freq2 = freqYear2.get(word);
		    }
		    similarity += freq1 * freq2;
		}
		similarity /= Math.sqrt(norm1) * Math.sqrt(norm2);

		distance.set(1 - similarity);
		context.write(key, distance);
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

	Job job = Job.getInstance(conf, "Cosine");
	job.setJarByClass(Cosine.class);
	job.setNumReduceTasks(NUM_REDUCERS);

	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(Text.class);

	job.setMapperClass(CosMapper.class);
	job.setReducerClass(CosReducer.class);

	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(DoubleWritable.class);

	FileInputFormat.setInputDirRecursive(job, true);

	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));
	MultipleOutputs.addNamedOutput(job, "CosineSimilarity",
		TextOutputFormat.class, Text.class, DoubleWritable.class);

	boolean done = job.waitForCompletion(true);

	if (done) {
	    System.exit(0);
	} else {
	    System.exit(1);
	}
    }
}
