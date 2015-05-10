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
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Computes a cosine based distance between a subset of articles from a year
 * with the years from the whole corpus.
 * 
 * @author Cynthia
 * 
 */
public class CosineArticles {
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
    private static class CosMapper extends
    Mapper<LongWritable, Text, Text, Text> {

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
	    String[] path1 = context.getConfiguration().get("inputDirArticles")
		    .split("/");
	    String[] path2 = context.getConfiguration()
		    .get("inputDirTotalYears").split("/");
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
	public void map(LongWritable key, Text line, Context context)
		throws IOException, InterruptedException {

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
     * Reducer to compute distance 1-cos_similarity: returns the distance for
     * each pair of years.
     * 
     * @author Cynthia
     * 
     */
    private static class CosReducer extends
    Reducer<Text, Text, Text, NullWritable> {

	private Text output = new Text();

	/**
	 * Outputs the distance 1-cos_similarity
	 */
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
		throws IOException, InterruptedException {

	    HashMap<String, Double> freqYearArticle = new HashMap<String, Double>();
	    HashMap<String, Double> freqYearDataset = new HashMap<String, Double>();
	    HashSet<String> words = new HashSet<String>();

	    // Store the frequencies for each words in both years in tables
	    // Store the distinct words in a HashSet
	    // Compute the norm for each year
	    Iterator<Text> valuesIt = values.iterator();
	    double frequency = 0;
	    while (valuesIt.hasNext()) {
		String[] val = valuesIt.next().toString().split("/");
		words.add(val[1]);
		frequency = Double.parseDouble(val[2]);
		if (val[0].equals("fromArticle")) {
		    freqYearArticle.put(val[1], frequency);
		}
		if (val[0].equals("fromCorpus")) {
		    freqYearDataset.put(val[1], frequency);
		}
	    }

	    // Computes the distance only if both years contain words
	    if (!freqYearArticle.isEmpty() && !freqYearDataset.isEmpty()) {
		double similarity = 0.0;
		double freq1, freq2;
		Iterator<String> wordsIt = words.iterator();
		double norm1 = 0.0;
		double norm2 = 0.0;
		while (wordsIt.hasNext()) {
		    freq1 = 0.0;
		    freq2 = 0.0;
		    String word = wordsIt.next();

		    // Cosine similarity:
		    if (freqYearArticle.containsKey(word)) {
			freq1 = (double) freqYearArticle.get(word);
			norm1 += Math.pow(freq1, 2);
		    }
		    if (freqYearDataset.containsKey(word)) {
			freq2 = (double) freqYearDataset.get(word); // remove counts from articles to have distinct sets
			norm2 += Math.pow(freq2, 2);
		    }
		    similarity += freq1 * freq2;
		}
		similarity /= Math.sqrt(norm1) * Math.sqrt(norm2);

		double distance = 1 - similarity;
		String[] years = key.toString().split(":");
		output.set(years[0]+","+years[1]+","+String.valueOf(distance));
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
    public static void main(String[] args) throws IOException,
    ClassNotFoundException, InterruptedException {

	String input1 = args[0];
	String input2 = args[1];

	Configuration conf = new Configuration();

	Job job = Job.getInstance(conf, "CosineWithTFIDF");
	job.setJarByClass(CosineArticles.class);
	job.setNumReduceTasks(NUM_REDUCERS);

	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(Text.class);

	job.setMapperClass(CosMapper.class);
	job.setReducerClass(CosReducer.class);

	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(NullWritable.class);

	job.getConfiguration().set("inputDirArticles", input1);
	job.getConfiguration().set("inputDirTotalYears", input2);

	FileInputFormat.setInputDirRecursive(job, true);

	FileInputFormat.addInputPaths(job, input1 + "," + input2);
	FileOutputFormat.setOutputPath(job, new Path(args[2]));
	MultipleOutputs.addNamedOutput(job, "CosineSimilarity",
		TextOutputFormat.class, Text.class, NullWritable.class);

	boolean done = job.waitForCompletion(true);

	if (done) {
	    System.exit(0);
	} else {
	    System.exit(1);
	}
    }
}