package ch.epfl.bigdata.ocr;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;

/**
 * For every word, generate a list of "hashes" by removing letters such as to gather bags of words
 * that result in the same hash. Equivalent to Levenshtein distance <= 2.
 * @author nicolas
 */
public class OcrGroupByHash {

	public static class GroupByHashMapper extends Mapper<Object, Text, Text, Text> {

		public void map(Object none, Text oneGramLine, Context context)
				throws IOException, InterruptedException {
			String[] both = oneGramLine.toString().split("\\s");
			String wordText = both[0];
			String wordPlusFreq = wordText + ":" + both[1];
			String word = wordText.toString();
			for (int i = 0; i < word.length(); i++) {
				// String withWildcard = word.substring(0,
				// i)+"?"+word.substring(i+1);
				String withWildcard = word.substring(0, i) + word.substring(i + 1);
				if(!withWildcard.trim().isEmpty()){
					context.write(new Text(withWildcard), new Text(wordPlusFreq));
				}
			}
			context.write(new Text(wordText), new Text(wordPlusFreq));
		}
	}

	public static class GroupByHashReducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Iterator<Text> iter = values.iterator();
			HashSet<String> alternatives = new HashSet<>();
			while (iter.hasNext()) {
				alternatives.add(iter.next().toString());
			}
			if (alternatives.size() > 1) {
				context.write(key, new Text(StringUtils.join(",", alternatives)));
			}	
		}
	}

	public static Job createGroupByHashJob(Path in, Path out) throws IOException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "OCR group by hash");
		job.setJarByClass(OcrGroupByHash.class);
		job.setMapperClass(GroupByHashMapper.class);
		job.setReducerClass(GroupByHashReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		out.getFileSystem(conf).delete(out, true);
		FileInputFormat.addInputPath(job, in);
		FileOutputFormat.setOutputPath(job, out);
		return job;
	}

	public static void main(String[] args) throws Exception {
		Job job = createGroupByHashJob(new Path(args[0]), new Path(args[1]));
		job.waitForCompletion(true);
		Job job1 = OcrAlternatives.createAlternativesJob(new Path(args[1]), new Path(args[2]));
		job1.waitForCompletion(true);
		ApplyCorrectionJob.setCorrectionPath(new Path(args[2]));
		Job job2 = ApplyCorrectionJob.createCorrectionJob(new Path(args[0]), new Path(args[3]));
		System.exit(job2.waitForCompletion(true) ? 0 : 1);
		
	}
}
