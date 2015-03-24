package ch.bigdata2015.linguisticdrift.tfidf;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class IDFFileOutputFormat extends FileOutputFormat<Text, DoubleWritable> {

	@Override
	public RecordWriter<Text, DoubleWritable> getRecordWriter(
			TaskAttemptContext job) throws IOException, InterruptedException {
		Configuration conf = job.getConfiguration();
		return new IDFRecordWriter(conf, getOutputPath(job));
	}
}
