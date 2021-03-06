package ch.bigdata2015.linguisticdrift.tfidf;

import java.io.IOException;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * TFIDF RecordWriter. Writes in a single file per year
 * 
 * @author Marc Schaer
 *
 */
public class TFIDFRecordWriter extends
		RecordWriter<IntWritable, Iterable<Text>> {

	private OutputStreamWriter out;
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
	public TFIDFRecordWriter(Configuration c, Path p) {
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
	public void write(IntWritable year, Iterable<Text> textList)
			throws IOException, InterruptedException {
		String fileName = year.toString();

		// Get the whole path for the output file
		Path file = new Path(this.path.getParent() + "/" + this.path.getName()
				+ "/" + fileName);
		FileSystem fs = file.getFileSystem(conf);
		
		try {
			if (fs.exists(file)) {
				
				out = new OutputStreamWriter(fs.append(file), "UTF-8");
			} else {
				
				out = new OutputStreamWriter(fs.create(file), "UTF-8");
			}
			for (Text t : textList) {
				byte[] tByte = t.getBytes();
				out.write(t.toString() + "\n");
			}
		} finally {
			if (out != null) {
				out.close();
			}
		}
	}
}
