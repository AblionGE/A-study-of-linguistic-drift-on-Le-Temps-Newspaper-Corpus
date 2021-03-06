package ch.epfl.bigdata.synonyms;

import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;

import ch.epfl.bigdata.ocr.YearAwareInputFormat;

/**
 * @author nicolas
 * Format temporal frequencies of ngrams in a more concise format instead of having an tuple for every
 * pair of (ngram, year). Year are grouped by range of STEP years from START to END and a vector of
 * frequencies is built by averaging each group. As this methods was used to gather the data about 
 * synonyms only, a file with all the useful words is provided such as to avoid retrieving all the ngrams.
 * 
 * Input :  ngrams/x-grams
 * 			a folder containing all the ngrams of interest (one per line)
 * 			N, the maximum [N]grams that must be looked up
 * Output : ngram	avg([freq(ngram,START), ... , freq(ngram, START+STEP)]);...;
 */
public class ExtractNgramVectors {
	
	public static class ExtractMapper extends Mapper<Text, Text, Text, Text> {
		
		private static Path correctionPath = new Path("/projects/linguistic-shift/synonyms-seed/");
		//private static Path correctionPath = new Path("filter");
		
		private HashSet<String> needed = new HashSet<>();
		
		@Override
		@SuppressWarnings(value = { "deprecation" })
		protected void setup(Context context) throws IOException ,InterruptedException {
			Configuration conf = context.getConfiguration();
			FileSystem fs = correctionPath.getFileSystem(conf);
			RemoteIterator<LocatedFileStatus> files = fs.listFiles(correctionPath, true);
			while(files.hasNext()){
				LocatedFileStatus file = files.next();
				FSDataInputStream is = fs.open(file.getPath());
				String line = is.readLine();
				while(line != null){
					needed.add(line);
					line = is.readLine();
				}
			}
			System.out.println(needed.size());
		};

		public void map(Text year, Text nGramLine, Context context)
				throws IOException, InterruptedException {
			String[] both = nGramLine.toString().split("\\s");
			String ngram = both[0];
			if(needed.contains(ngram)){
				context.write(new Text(ngram), new Text(year.toString()+":"+Integer.parseInt(both[1])));
			}
		}
	}
	
	public static class ExtractReducer extends Reducer<Text, Text, Text, Text> {
		
		
		private static final int START = 1840;
		private static final int END = 1998;
		private static final double STEP = 5.;
		
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			int bags = (int)Math.ceil((END-START)/STEP);
			System.out.println(bags);
			int[] freqs = new int[bags];
			int[] divs = new int[bags];
			String ngram = key.toString();
			for(Text freq:values){
				String[] parts = freq.toString().split(":");
				int year = Integer.parseInt(parts[0].replace(".", ""));
				int bag = (int)Math.floor((year-START)/STEP);
				int frequency = Integer.parseInt(parts[1]);
				freqs[bag] += frequency;
				divs[bag]++;
			}
			String[] concat = new String[bags];
			for(int i=0; i<bags; i++){
				int div = (divs[i] == 0 ? 1 : divs[i]);
				concat[i] = new Integer((int)Math.ceil(freqs[i]/div)).toString();
			}
			context.write(new Text(ngram), new Text(StringUtils.join(";", concat)));
		}
	}

	public static Job createExtractionJob(Path in, Path out, int n) throws IOException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Replace in n-grams");
		job.setJarByClass(ExtractNgramVectors.class);
		job.setMapperClass(ExtractMapper.class);
		job.setReducerClass(ExtractReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(1);

		job.setInputFormatClass(YearAwareInputFormat.class);
		out.getFileSystem(conf).delete(out, true);
		String[] paths = new String[n];
		for(int i=1; i<=n; i++){
			paths[i-1] = in.toString()+Path.SEPARATOR+i+"-grams";
		}
		System.out.println(StringUtils.join(",", paths));
		FileInputFormat.addInputPaths(job, StringUtils.join(",", paths));
		FileOutputFormat.setOutputPath(job, out);
		return job;
	}
	
	public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException{
		if(args.length != 4){
			System.out.println("4 arguments expected : rootDirForNgrams output dirWithNeededNgramList n:int");
			System.exit(1);
		}
		Job job =  createExtractionJob(new Path(args[0]), new Path(args[1]), Integer.parseInt(args[3]));
		//ExtractNgramVectors.ExtractMapper.setCorrectionPath(new Path(args[2]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
