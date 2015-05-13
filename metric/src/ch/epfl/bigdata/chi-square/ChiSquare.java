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
 * 
 * @author Cynthia
 * 
 */
public class ChiSquare {
    private static final int NUM_REDUCERS = 25;

    /**
     * Mapper to compute chi-square distance: takes a directory with all 1-gram
     * files as input. For one year y1 and for each words in y1, it returns a
     * tuple for each y2 in [1840,1998]: (y1:y2, y1/word/frequency).
     * 
     * @author Cynthia
     * 
     */
    private static class CSMapper extends Mapper<LongWritable, Text, Text, Text> {

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
	public void map(LongWritable key, Text line, Context context) throws IOException, InterruptedException {

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
     * Reducer to compute chi-square distance: returns the distance for each
     * pair of years.
     * 
     * @author Cynthia
     * 
     */
    private static class CSReducer extends Reducer<Text, Text, Text, DoubleWritable> {

	private HashMap<String, Integer> wordOccurences = null;
	private DoubleWritable distance = new DoubleWritable();

	/**
	 * Read the files containing the terms needed for chi-square: -
	 * yearOccurences are the number of words appearing in each year -
	 * wordOccurences are the number of times each word appear in the whole
	 * corpus
	 */
	@Override
	public void setup(Context context) throws IOException {
	    Path pt = new Path("/projects/linguistic-shift/stats/WordOccurenceOverAllYears");
	    FileSystem hdfs = pt.getFileSystem(context.getConfiguration());
	    if (hdfs.isFile(pt)) {
		FSDataInputStream fis = hdfs.open(pt);
		BufferedReader br = new BufferedReader(new InputStreamReader(fis));
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

	/**
	 * Outputs the chi-square distance
	 */
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

	    HashMap<String, Double> freqYear1 = new HashMap<String, Double>();
	    HashMap<String, Double> freqYear2 = new HashMap<String, Double>();
	    HashSet<String> words = new HashSet<String>();

	    // Store the frequencies for each words in both years in tables
	    // Store the distinct words in a HashSet
	    Iterator<Text> valuesIt = values.iterator();
	    String[] years = key.toString().split(":");
	    String year1 = years[0];
	    String year2 = years[1];
	    double wordCount1 = 0.0;
	    double wordCount2 = 0.0;
	    double frequency = 0.0;
	    double prev;
	    String w;

	    while (valuesIt.hasNext()) {
		String[] val = valuesIt.next().toString().split("/");
		w = val[1];
		frequency = Double.parseDouble(val[2]);
		words.add(val[1]);
		prev = 0.0;
		if (val[0].equals(year1)) {
		    if(freqYear1.containsKey(w)){
			prev = freqYear1.get(w);
		    }
		    freqYear1.put(w, prev+frequency);
		    wordCount1 += frequency;
		}
		prev = 0.0;
		if (val[0].equals(year2)) {
		    if(freqYear2.containsKey(w)){
			prev = freqYear2.get(w);
		    }
		    freqYear2.put(w, prev+frequency);
		    wordCount2 += frequency;
		}
	    }

	    // Computes the distance only if both years contain words
	    if (!freqYear1.isEmpty() && !freqYear2.isEmpty()) {
		Iterator<String> wordsIt = words.iterator();
		double dist = 0.0;
		double freq1, freq2;
		while (wordsIt.hasNext()) {
		    String word = wordsIt.next();
		    double wordOccurence = (double) wordOccurences.get(word);
		    freq1 = 0.0;
		    freq2 = 0.0;

		    // Chi-Square distance
		    if (freqYear1.containsKey(word)) {
			freq1 = freqYear1.get(word);
		    }
		    if (freqYear2.containsKey(word)) {
			freq2 = freqYear2.get(word);
		    }
		    dist += Math.pow(freq1 / wordCount1 - freq2 / wordCount2, 2) / wordOccurence;
		}
		distance.set(dist);
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
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

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
	MultipleOutputs.addNamedOutput(job, "ChiSquare", TextOutputFormat.class, Text.class, DoubleWritable.class);

	boolean done = job.waitForCompletion(true);

	if (done) {
	    System.exit(0);
	} else {
	    System.exit(1);
	}
    }
}
