import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class GunZip {

  public static class TokenizerMapper extends Mapper<Text, BytesWritable, Text, BytesWritable>{

    public void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
        context.write(key, value);
      }
  }

  public static class IntSumReducer extends Reducer<Text,BytesWritable,Text,BytesWritable> {

    public void reduce(Text key, Iterable<BytesWritable> values, Context context) throws IOException, InterruptedException {
      context.write(key, values.iterator().next());
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(GunZip.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setInputFormatClass(CustomGzipFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(BytesWritable.class);
    job.setOutputFormatClass(BinaryOutputFormat.class);
    CustomGzipFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
    System.out.println("done");
  }
}

class CustomGzipFormat extends FileInputFormat<Text, BytesWritable>{

	//gzip format is inherently sequential
	@Override
	protected boolean isSplitable(JobContext context, Path filename) { return false;};
	
	@Override
	public RecordReader<Text, BytesWritable> createRecordReader(InputSplit arg0, TaskAttemptContext arg1) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return new RecordReader<Text, BytesWritable>(){
			@Override
			public void close() throws IOException {}
			@Override
			public Text getCurrentKey() throws IOException, InterruptedException {
				return nextKey;
			}

			@Override
			public BytesWritable getCurrentValue() throws IOException,InterruptedException {
				return nextValue;
			}

			@Override
			public float getProgress() throws IOException, InterruptedException {
				return next ? 0 : 1;
			}

			private Text nextKey;
			private BytesWritable nextValue = new BytesWritable();
			private final int STEP = 1024;
			
			@Override
			public void initialize(InputSplit split, TaskAttemptContext attempt) throws IOException, InterruptedException {
				Path inputPath = ((FileSplit)split).getPath();
				FSDataInputStream fsIs = inputPath.getFileSystem(attempt.getConfiguration()).open(inputPath);
				//set filename as key
				nextKey = new Text(inputPath.getName().replace(".gz", ""));
				//extract gunziped file content
				byte[] data = new byte[STEP];
				GZIPInputStream gzipIs = new GZIPInputStream(fsIs);
				ByteArrayOutputStream buffer = new ByteArrayOutputStream();
				int available = -1;
				do{
					available = gzipIs.read(data);
					if(available > 0){
						buffer.write(data, 0, available);
					}
				} while (available > 0);
				byte[] fullFile = buffer.toByteArray();
				nextValue.set(fullFile, 0, fullFile.length);
			}

			private boolean next = true;
			@Override
			public boolean nextKeyValue() throws IOException,
					InterruptedException {
				// TODO Auto-generated method stub
				boolean temp = next;
				next = false;
				return temp;
			}
			
		};
	}
}

class BinaryOutputFormat extends FileOutputFormat<Text, BytesWritable>{
	
	@Override
	public RecordWriter<Text, BytesWritable> getRecordWriter(final TaskAttemptContext context) throws IOException, InterruptedException {
		return new RecordWriter<Text, BytesWritable>(){

			@Override
			public void close(TaskAttemptContext arg0) throws IOException, InterruptedException {}
			
			@Override
			public void write(Text fileName, BytesWritable content) throws IOException, InterruptedException {
				Path target = new Path(getOutputPath(context), fileName.toString());
				FSDataOutputStream output = target.getFileSystem(context.getConfiguration()).create(target);
				output.write(content.getBytes());
				output.flush();
				output.close();
			}
		};
	}
	
}
