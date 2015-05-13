package ch.epfl.bigdata.synonyms.metrics;

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
 * Almost the exact same behaviour as {@link ExtractNgramVectors} except that is retrieves all the
 * 1-grams regardless of whether they actually are part of the filter file.
 * TODO : maybe not useful anymore
 */
public class DateWithChanging {
	
	public static class ExtractMapper extends Mapper<Text, Text, Text, Text> {
		
		private static Path correctionPath = new Path("/projects/linguistic-shift/synonyms-seed/");
		//private static Path correctionPath = new Path("filter");
		
		private HashSet<String> needed = new HashSet<>();
		
		@SuppressWarnings("deprecation")
		@Override
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
		};

		public void map(Text year, Text nGramLine, Context context)
				throws IOException, InterruptedException {
			String[] both = nGramLine.toString().split("\\s");
			String ngram = both[0];
			/**
			 * Meaning it's a one-gram
			 */
			if(needed.contains(ngram) || !ngram.contains(",")){
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
		job.setJarByClass(DateWithChanging.class);
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
