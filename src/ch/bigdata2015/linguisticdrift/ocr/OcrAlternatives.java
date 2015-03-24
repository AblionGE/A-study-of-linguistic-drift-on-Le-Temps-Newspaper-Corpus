package ch.bigdata2015.linguisticdrift.ocr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.LoggerFactory;
import java.util.*;

public class OcrAlternatives {

	private static class Freq{
		public final String word;
		public final int occurrences;
		public Freq(String word, int occurences){
			this.word = word;
			this.occurrences = occurences;
		}
		@Override
		public String toString(){
			return word+":"+occurrences;
		}
	}
	
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
			
			Freq[] freqs = new Freq[entries.length];
			for(int i = 0; i < entries.length; i++){
				String[] temp = entries[i].split(":");
				freqs[i] = new Freq(temp[0], Integer.parseInt(temp[1]));
			}	
			
			for (int i = 0; i < entries.length; i++) {
				for(int j = i+1; j < entries.length; j++){
					if(freqs[i].occurrences < freqs[j].occurrences){
						context.write(new Text(freqs[i].toString()), new Text(freqs[j].toString()));
					} else {
						context.write(new Text(freqs[j].toString()), new Text(freqs[i].toString()));
					}
				}
			}
		}
	}

	public static class AlternativesReducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Iterator<Text> iter = values.iterator();
			HashSet<String> alternatives = new HashSet<>();
			while (iter.hasNext()) {
				alternatives.add(iter.next().toString());
			}
			context.write(key,new Text(StringUtils.join(",", alternatives)));
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
