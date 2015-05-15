package ch.epfl.bigdata.appearingwords;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
public class GatherChangingWords {
	
	public static class ExtractMapper extends Mapper<Text, Text, Text, Text> {

		public void map(Text year, Text nGramLine, Context context)
				throws IOException, InterruptedException {
			String[] both = nGramLine.toString().split("\\s");
			String ngram = both[0];
			context.write(new Text(ngram), new Text(year.toString()+":"+Integer.parseInt(both[1])));
		}
	}
	
	public static class ExtractReducer extends Reducer<Text, Text, Text, Text> {
		
		private static final int START = 1840;
		private static final int END = 1998;
		private static final double STEP = 5.;
		private static int LOW_TRESHOLD;
		private static int HIGH_TRESHOLD;
		private static double RATIO;
		private final static double[] normalize = new double[]{
				0.19464584812153785, 0.19378179083482383, 0.2933894544425897, 0.45685983968064375, 0.47906314923515303, 0.4555128194201441, 0.6182003176191793,
				0.6238647833545796, 0.5868369456122814, 0.5250481238106022, 0.5894356110922391, 0.6447929083177993, 0.7620051810969104, 0.9141608396575103, 1,
				0.4418652598239336, 0.6589258659952264, 0.6195660070366488, 0.629975685452303, 0.5852898406912134, 0.5655492364517725, 0.6061323310431148,
				0.5143425798420995, 0.5430541198567994, 0.6358520055341875, 0.7211935922583065, 0.7527006356036245, 0.6463156621645588, 0.8275120060942622,
				0.8893353252512117, 0.7216810195957154, 0.42435318473496086
			};
		
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			super.setup(context);
			Configuration cf = context.getConfiguration();
			LOW_TRESHOLD = Integer.parseInt(cf.get("low"));
			HIGH_TRESHOLD = Integer.parseInt(cf.get("high"));
			RATIO = Double.parseDouble(cf.get("ratio"));
		}
		
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			int bags = (int)Math.ceil((END-START)/STEP);
			int[] freqs = new int[bags];
			/*
			 * Count the number of year actually in the group, to normalize extreme cases.
			 */
			int[] divs = new int[bags];
			String ngram = key.toString();
			int total = 0;
			
			/*
			 *	Compute the histogram of mean frequencies over the STEP interval 
			 */
			for(Text freq:values){
				String[] parts = freq.toString().split(":");
				int year = Integer.parseInt(parts[0].replace(".", ""));
				int bag = (int)Math.floor((year-START)/STEP);
				int frequency = Integer.parseInt(parts[1]);
				total += frequency;
				freqs[bag] += frequency;
				divs[bag]++;
			}
			
			String[] concat = new String[bags];
			int amountAboveTheshold = 0;
			int amountBelowTheshold = 0;
			int zerosSeqAtStart = 0;
			
			for(int i=0; i<bags; i++){
				int div = (divs[i] == 0 ? 1 : divs[i]);
				int freq = (int)Math.ceil((freqs[i]/div)/normalize[i]);
				
				amountAboveTheshold += (freq>=HIGH_TRESHOLD ? 1 : 0);
				amountBelowTheshold += (freq<=LOW_TRESHOLD ? 1 : 0);
				zerosSeqAtStart += (zerosSeqAtStart == i && freq<=LOW_TRESHOLD ? 1 : 0);

				concat[i] = new Integer(freq).toString();
			}
			int appear = (int)(START + zerosSeqAtStart*STEP);
			
			if(amountAboveTheshold > 1 && (amountAboveTheshold >= (int)Math.ceil((bags-zerosSeqAtStart)*RATIO)) && appear > 1840){
				context.write(new CustomText(ngram+":"+appear), new Text(StringUtils.join(";", concat)));
			}
		}
	}

	public static Job createExtractionJob(Path in, Path out, String low, String high, String ratio) throws IOException {
		Configuration conf = new Configuration();
		conf.set("low", low);
		conf.set("high", high);
		conf.set("ratio", ratio);
		
		Job job = Job.getInstance(conf, "Replace in n-grams");
		job.setJarByClass(GatherChangingWords.class);
		job.setMapperClass(ExtractMapper.class);
		job.setReducerClass(ExtractReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(1);

		job.setInputFormatClass(YearAwareInputFormat.class);
		out.getFileSystem(conf).delete(out, true);
		int n = 1;
		String[] paths = new String[n];
		for(int i=1; i<=n; i++){
			paths[i-1] = in.toString()+Path.SEPARATOR+i+"-grams";
		}
		FileInputFormat.addInputPaths(job, StringUtils.join(",", paths));
		FileOutputFormat.setOutputPath(job, out);
		return job;
	}
	
	public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException{
		if(args.length != 5){
			System.out.println("4 arguments expected : rootDirForNgrams output low:int high:int");
			System.exit(1);
		}
		Job job =  createExtractionJob(new Path(args[0]), new Path(args[1]), args[2], args[3], args[4]);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
