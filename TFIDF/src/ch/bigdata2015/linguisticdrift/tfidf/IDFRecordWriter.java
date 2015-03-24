package ch.bigdata2015.linguisticdrift.tfidf;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class IDFRecordWriter extends RecordWriter<Text, DoubleWritable> {

	private DataOutputStream out;
	private Configuration conf;
	private Path path;
	private final static String FILENAME = "idf";

	public IDFRecordWriter(Configuration conf, Path path) {
		this.conf = conf;
		this.path = path;
	}

	@Override
	public void close(TaskAttemptContext context) throws IOException,
			InterruptedException {
		if (out != null) {
			out.close();
		}
	}

	@Override
	public void write(Text str, DoubleWritable value) throws IOException,
			InterruptedException {
		// I remove the last part of the name which is
		// a counter of each part of the file
		String fileName = FILENAME;

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
			String word = str.toString();
			Double occurrences = value.get();
			out.writeChars(word + "\t" + occurrences.toString() + "\n");
		} finally {
			if (out != null) {
				out.close();
			}
		}
	}
}
