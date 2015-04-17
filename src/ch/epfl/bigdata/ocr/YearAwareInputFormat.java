package ch.epfl.bigdata.ocr;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class YearAwareInputFormat extends FileInputFormat<Text, Text>{
	@Override
	protected boolean isSplitable(JobContext context, Path filename) { return false;};
	
	@Override
	public RecordReader<Text, Text> createRecordReader(InputSplit arg0, TaskAttemptContext arg1) throws IOException, InterruptedException {
		return new RecordReader<Text, Text>(){
			@Override
			public void close() throws IOException {}
			@Override
			public Text getCurrentKey() throws IOException, InterruptedException {
				return fileName;
			}

			@Override
			public Text getCurrentValue() throws IOException,InterruptedException {
				return new Text(fsIs.readLine());
			}

			@Override
			public float getProgress() throws IOException, InterruptedException {
				return next ? 0 : 1;
			}

			private Text fullInputPath;
			private Text fileName;
			private FSDataInputStream fsIs;
			
			@Override
			public void initialize(InputSplit split, TaskAttemptContext attempt) throws IOException, InterruptedException {
				Path inputPath = ((FileSplit)split).getPath();
				fsIs = inputPath.getFileSystem(attempt.getConfiguration()).open(inputPath);
				fullInputPath = new Text(inputPath.toString());
				fileName = new Text(inputPath.getName().replaceAll("[a-zA-z]+", ""));
			}
			private boolean next = true;
			@Override
			public boolean nextKeyValue() throws IOException,
					InterruptedException {
				return fsIs.available() > 0;
			}
			
		};
	}
}
