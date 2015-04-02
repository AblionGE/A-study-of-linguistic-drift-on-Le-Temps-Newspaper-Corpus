package ch.bigdata2015.linguisticdrift.tfidf;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Record Writer for TFTot.
 * 
 * @author Marc Schaer
 *
 */
public class TFTotRecordWriter extends RecordWriter<Text, IntWritable> {

	private DataOutputStream out;
	private Configuration conf;
	private Path path;

	/**
	 * Constructor.
	 * 
	 * @param c
	 *            the configuration
	 * @param p
	 *            Path of the file
	 */
	public TFTotRecordWriter(Configuration c, Path p) {
		this.conf = c;
		this.path = p;
	}

	@Override
	public void close(TaskAttemptContext context) throws IOException,
			InterruptedException {
		if (out != null) {
			out.close();
		}
	}

	@Override
	public void write(Text year, IntWritable occurences) throws IOException,
			InterruptedException {
		String fileName = "YearOccurences";

		// Get the whole path for the output file
		Path file = new Path(this.path.getParent() + "/" + this.path.getName()
				+ "/" + fileName);
		FileSystem fs = file.getFileSystem(conf);
		try {
			if (fs.exists(file)) {
				out = fs.append(file);
			} else {
				out = fs.create(file);
			}
			out.writeBytes(year.toString() + "\t" + occurences.toString() + "\n");
		} finally {
			if (out != null) {
				out.close();
			}
		}
	}

}
