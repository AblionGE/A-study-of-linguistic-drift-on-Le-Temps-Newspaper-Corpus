//package ch.bigdata2015.linguisticdrift.tfidf;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * TFIDF FileOutputFormat.
 * @author Marc Schaer
 *
 */
public class TFIDFFileOutputFormat extends FileOutputFormat<IntWritable, Iterable<Text>> {

	@Override
	public RecordWriter<IntWritable, Iterable<Text>> getRecordWriter(TaskAttemptContext job)
			throws IOException, InterruptedException {
		Configuration conf = job.getConfiguration();
		return new TFIDFRecordWriter(conf, getOutputPath(job));
	}

}
