package ch.epfl.bigdata.ocr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 1 grams are computer on a yearly basis, simply merge everything to have the overall frequencies of OCR correction
 * @author nicolas
 *
 */
public class Merge1Grams {

	public static class MergerMapper extends Mapper<Object, Text, Text, IntWritable> {

		public void map(Object none, Text oneGramLine, Context context) throws IOException, InterruptedException {
			String[] both = oneGramLine.toString().split("\\s");
			context.write(new Text(both[0]), new IntWritable(Integer.parseInt(both[1])));
		}
	}

	public static class MergerReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int total = 0;
			for(IntWritable frequency: values){
				total += frequency.get();
			}
			context.write(key, new IntWritable(total));
		}
	}

	public static Job createMergerJob(Path in, Path out) throws IOException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Merge 1 grams");
		job.setJarByClass(Merge1Grams.class);
		job.setMapperClass(MergerMapper.class);
		job.setReducerClass(MergerReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setNumReduceTasks(1);

		out.getFileSystem(conf).delete(out, true);
		FileInputFormat.addInputPath(job, in);
		FileOutputFormat.setOutputPath(job, out);
		return job;
	}

}
