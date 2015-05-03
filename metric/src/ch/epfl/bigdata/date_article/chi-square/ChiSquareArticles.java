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
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.util.List;

/**
 * Computes Chi-Square distance between a subset of articles from a year with
 * the years from the whole corpus.
 * 
 * @author Cynthia
 * 
 */
public class ChiSquareArticles {
    private static final int NUM_REDUCERS = 25;

    /**
     * Mapper: takes a directory with the n-gram files for the subset of
     * articles and a directory with all n-gram files as input. For the year of
     * the articles y1 and for each words in y1, it returns a tuple for each y2
     * in [1840,1998]: (y1:y2, "fromArticle"/word/frequency). For each year y2
     * and each words in y2, it returns a tuple: (y1:y2,
     * "fromCorpus"/word/frequency).
     * 
     * @author Cynthia
     * 
     */
    private static class CSMapper extends Mapper<LongWritable, Text, Text, Text> {

	private final int firstYear = 1840;
	private final int lastYear = 1998;
	private String articlesDir;
	private String yearsDir;
	private Text years = new Text();
	private Text values = new Text();

	/**
	 * Setup the names of both input directories to differentiate words
	 * coming from the subset of articles and words coming from the corpus.
	 */
	@Override
	public void setup(Context context) throws IOException {
	    String[] path1 = context.getConfiguration().get("inputDirArticles").split("/");
	    String[] path2 = context.getConfiguration().get("inputDirTotalYears").split("/");
	    articlesDir = path1[path1.length - 1];
	    yearsDir = path2[path2.length - 1];
	}

	/**
	 * The function extracts the name of the file from which the line comes
	 * and determine the year corresponding to it. If the line comes from
	 * the subset of articles, then a key-value pair is returned for each
	 * year in the corpus. If the line comes from the corpus, a single
	 * key-value pair is returned.
	 */
	@Override
	public void map(LongWritable key, Text line, Context context) throws IOException, InterruptedException {

	    // Get file name informations
	    FileSplit splitInfo = (FileSplit) context.getInputSplit();
	    String[] filePath = splitInfo.getPath().toString().split("/");
	    String fileDir = filePath[filePath.length - 2];

	    // Extract the year and the type of the line depending from which
	    // file it comes
	    String year = "";
	    String type = "";
	    if (fileDir.equals(articlesDir)) {
		year = fileDir;
		type = "fromArticle";
	    } else if (fileDir.equals(yearsDir)) {
		String fileName = splitInfo.getPath().getName();
		year = fileName.replaceAll("-r-[0-9]+", "");
		type = "fromCorpus";
	    }

	    // Build the output depending from which file the line comes
	    String[] tokens = line.toString().split("\\s+");
	    if (tokens.length == 2) {
		String word = tokens[0];
		String frequency = tokens[1];
		values.set(type + "/" + word + "/" + frequency);
		if (fileDir.equals(articlesDir)) {
		    for (int i = firstYear; i <= lastYear; i++) {
			years.set(year + ":" + i);
			context.write(years, values);
		    }
		} else if (fileDir.equals(yearsDir)) {
		    years.set(articlesDir + ":" + year);
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
    private static class CSReducer extends Reducer<Text, Text, Text, NullWritable> {

	private HashMap<String, Integer> wordOccurences = null;
	private Text output = new Text();

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

	    HashMap<String, Integer> freqYearArticle = new HashMap<String, Integer>();
	    HashMap<String, Integer> freqYearDataset = new HashMap<String, Integer>();
	    HashSet<String> words = new HashSet<String>();

	    // Store the frequencies for each words in both years in tables
	    // Store the distinct words in a HashSet
	    Iterator<Text> valuesIt = values.iterator();
	    double wordCountArticle = 0.0;
	    double wordCountDataset = 0.0;
	    int frequency = 0;

	    while (valuesIt.hasNext()) {
		String[] val = valuesIt.next().toString().split("/");
		words.add(val[1]);
		frequency = Integer.parseInt(val[2]);
		if (val[0].equals("fromArticle")) {
		    freqYearArticle.put(val[1], frequency);
		    wordCountArticle += frequency;
		}
		if (val[0].equals("fromCorpus")) {
		    freqYearDataset.put(val[1], frequency);
		    wordCountDataset += frequency;
		}
	    }
	    // To not count the words from articles in the dataset
	    wordCountDataset -= wordCountArticle;

	    // Computes the distance only if both years contain words
	    if (!freqYearArticle.isEmpty() && !freqYearDataset.isEmpty()) {
		Iterator<String> wordsIt = words.iterator();
		double dist = 0.0;
		double freq1, freq2;
		while (wordsIt.hasNext()) {
		    String word = wordsIt.next();
		    freq1 = 0.0;
		    freq2 = 0.0;

		    // Chi-Square distance:
		    if (freqYearArticle.containsKey(word)) {
			freq1 = (double) freqYearArticle.get(word);
		    }
		    if (freqYearDataset.containsKey(word)) {
			freq2 = (double) freqYearDataset.get(word) - freq1; // -freq1 to not count the articles in the dataset
		    }
		    double wordOccurence = (double) wordOccurences.get(word);
		    dist += Math.pow(freq1 / wordCountArticle - freq2 / wordCountDataset, 2) / wordOccurence;
		}
		String[] years = key.toString().split(":");
		output.set(years[0] + "," + years[1] + "," + String.valueOf(dist));
		context.write(output, NullWritable.get());
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

	String input1 = args[0];
	String input2 = args[1];

	Configuration conf = new Configuration();

	Job job = Job.getInstance(conf, "ChiSquare");
	job.setJarByClass(ChiSquareArticles.class);
	job.setNumReduceTasks(NUM_REDUCERS);

	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(Text.class);

	job.setMapperClass(CSMapper.class);
	job.setReducerClass(CSReducer.class);

	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(NullWritable.class);

	job.getConfiguration().set("inputDirArticles", input1);
	job.getConfiguration().set("inputDirTotalYears", input2);

	FileInputFormat.setInputDirRecursive(job, true);

	FileInputFormat.addInputPaths(job, input1 + "," + input2);
	FileOutputFormat.setOutputPath(job, new Path(args[2]));
	MultipleOutputs.addNamedOutput(job, "ChiSquare", TextOutputFormat.class, Text.class, NullWritable.class);

	boolean done = job.waitForCompletion(true);

	if (done) {
	    System.exit(0);
	} else {
	    System.exit(1);
	}
    }
}