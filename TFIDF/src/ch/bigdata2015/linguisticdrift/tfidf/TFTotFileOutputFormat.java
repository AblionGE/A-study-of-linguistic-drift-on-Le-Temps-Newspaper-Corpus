package ch.bigdata2015.linguisticdrift.tfidf;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * FileOutputFormat for TfTot.
 * @author Marc Schaer
 *
 */
public class TFTotFileOutputFormat extends FileOutputFormat<Text, IntWritable> {

	@Override
	public RecordWriter<Text, IntWritable> getRecordWriter(TaskAttemptContext job)
			throws IOException, InterruptedException {
		Configuration conf = job.getConfiguration();
		return new TFTotRecordWriter(conf, getOutputPath(job));
	}

}
