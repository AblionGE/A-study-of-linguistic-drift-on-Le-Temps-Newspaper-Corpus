package ch.epfl.bigdata.ocr;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * @author nicolas
 * Extract the current year from the filename when reading n-grams and
 * return it as key.
 */
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
				return year;
			}

			@SuppressWarnings("deprecation")
			@Override
			public Text getCurrentValue() throws IOException,InterruptedException {
				return new Text(fsIs.readLine());
			}

			@Override
			public float getProgress() throws IOException, InterruptedException {
				return next ? 0 : 1;
			}

			private Text year;
			private FSDataInputStream fsIs;
			
			@Override
			public void initialize(InputSplit split, TaskAttemptContext attempt) throws IOException, InterruptedException {
				Path inputPath = ((FileSplit)split).getPath();
				fsIs = inputPath.getFileSystem(attempt.getConfiguration()).open(inputPath);
				new Text(inputPath.toString());
				year = new Text(inputPath.getName().replaceAll("[^\\d]+", "").substring(0, 4));
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
