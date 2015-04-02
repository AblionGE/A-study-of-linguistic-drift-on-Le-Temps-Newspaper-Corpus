package ch.epfl.bigdata.ocr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Using the result of {@link OcrGroupByHash}, list the replacement alternatives for every rare word
 * @author nicolas
 *
 */
public class OcrAlternatives {
	
	private static final int RARE_THRESHOLD = 5;
	//private static final int CORRECT_THRESHOLD = 100;
	
	/**
	 * Output all possible pairs of words from the right part, the left component of the pair having a lower (or equal) occurrence value
	 * @param Text oneGramLine : values of the form : hash\t(word:occurences){2,}
	 * @author nicolas
	 */
	public static class AlternativesMapper extends Mapper<Object, Text, Text, Text> {

		public void map(Object none, Text oneGramLine, Context context)
				throws IOException, InterruptedException {
			String[] both = oneGramLine.toString().split("\\s");
			String[] entries = both[1].split(",");
			//LoggerFactory.getLogger(this.getClass()).info(oneGramLine.toString());
			
			WordFreqPair[] freqs = new WordFreqPair[entries.length];
			for(int i = 0; i < entries.length; i++){
				freqs[i] = WordFreqPair.fromString(entries[i]);
			}	
			
			/*
			 * Output every possible pair of word, as long as they don't both occur rarely
			 */
			for (int i = 0; i < entries.length; i++) {
				for(int j = i+1; j < entries.length; j++){
				    WordFreqPair max = (freqs[i].occurrences > freqs[j].occurrences ? freqs[i] : freqs[j]);
				    WordFreqPair min = (max.occurrences == freqs[i].occurrences ? freqs[j] : freqs[i]);
					if(max.occurrences > RARE_THRESHOLD && min.occurrences < RARE_THRESHOLD){
						context.write(new Text(min.toString()), new Text(max.toString()));
					}
				}
			}
		}
	}

	public static class AlternativesReducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			
			WordFreqPair max = new WordFreqPair("", -1);
			for(Text t: values){
				WordFreqPair pair = WordFreqPair.fromString(t.toString());
				if(pair.occurrences > max.occurrences){
					max = pair;
				}
			}
			context.write(key,new Text(max.toString()));
		}
	}

	public static Job createAlternativesJob(Path in, Path out) throws IOException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "OCR build alternative list");
		job.setJarByClass(OcrAlternatives.class);
		job.setMapperClass(AlternativesMapper.class);
		job.setReducerClass(AlternativesReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);
		out.getFileSystem(conf).delete(out, true);
		FileInputFormat.addInputPath(job, in);
		FileOutputFormat.setOutputPath(job, out);
		return job;
	}
}
