
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * A custom FileInputFormat for XML, this format is not splittable.
 * @author gbrechbu
 *
 */
public class XmlFileInputFormat extends FileInputFormat<IntWritable, Text> {

	@Override
	public RecordReader<IntWritable, Text> createRecordReader(InputSplit arg0,
			TaskAttemptContext arg1) throws IOException, InterruptedException {
		return new XmlRecordReader();
	}
	
	@Override
	protected boolean isSplitable(JobContext context, Path file) {
		return false;
	}
}
