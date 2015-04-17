package ch.epfl.bigdata.ocr;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.StringUtils;

public class ApplyCorrectionJob {
	
	private static Path correctionPath;
	
	public static void setCorrectionPath(Path p){
		correctionPath = p;
	}

	public static class ReplacerMapper extends Mapper<Text, Text, Text, IntWritable> {
		
		private HashMap<String, String> corrs = new HashMap<>();
		
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
					String[] both = line.split("\\s");
					if(both.length == 2){
						corrs.put(both[0].split(":")[0], both[1].split(":")[0]);
						line = is.readLine();	
					}
				}
			}
		};

		public void map(Text year, Text nGramLine, Context context)
				throws IOException, InterruptedException {
			String[] both = nGramLine.toString().split("\\s");
			String[] grams = both[0].split(",");
			for(int i=0; i<grams.length; i++){
				String replacement = corrs.get(grams[i]);
				if(replacement != null){
					grams[i] = replacement;
				}
			}
			context.write(new Text(year.toString()+":"+StringUtils.join(",", grams).trim()), new IntWritable(Integer.parseInt(both[1])));
			
		}
	}
	
	public static class ReplacerReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		
		private MultipleOutputs<Text, IntWritable> mout;
		
		@Override
		public void setup(Context context) {
			 mout = new MultipleOutputs<Text, IntWritable>(context);
		}
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			String[] parts = key.toString().split(":");
			String year = parts[0];
			String ngram = parts[1];
			for(IntWritable freq:values){
				sum += freq.get();
			}
			mout.write("Output", new Text(ngram), new IntWritable(sum), year);
		}
		
		@Override
		public void cleanup(Context context) {
			try {
				mout.close();
			} catch (IOException | InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	public static Job createCorrectionJob(Path in, Path out) throws IOException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Replace in n-grams");
		job.setJarByClass(OcrAlternatives.class);
		job.setMapperClass(ReplacerMapper.class);
		job.setReducerClass(ReplacerReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setNumReduceTasks(1);

		job.setInputFormatClass(YearAwareInputFormat.class);
		out.getFileSystem(conf).delete(out, true);
		FileInputFormat.addInputPath(job, in);
		FileOutputFormat.setOutputPath(job, out);
		MultipleOutputs.addNamedOutput(job, "Output", TextOutputFormat.class, Text.class, Text.class);
		return job;
	}
}
