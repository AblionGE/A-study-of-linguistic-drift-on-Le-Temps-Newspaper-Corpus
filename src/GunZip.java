import java.io.IOException;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class GunZip {

  public static class GunzipMapper extends Mapper<Text, Text, Text, BytesWritable>{

    public void map(Text fileName, Text fullPath, Context context) throws IOException, InterruptedException {
    	final int STEP = 4096;
    	Path inputPath = new Path(fullPath.toString());
    	
    	FSDataInputStream fsIs = inputPath.getFileSystem(context.getConfiguration()).open(inputPath);
    	Path outputFile = new Path(FileOutputFormat.getOutputPath(context), fileName.toString());
    	FSDataOutputStream output = outputFile.getFileSystem(context.getConfiguration()).create(outputFile);
    	
		byte[] data = new byte[STEP];
		GZIPInputStream gzipIs = new GZIPInputStream(fsIs);
		int available = -1;
		do{
			available = gzipIs.read(data);
			if(available > 0){
				output.write(data, 0, available);
			}
		} while (available > 0);
		
		output.flush();
		output.close();
		
        context.write(fileName, new BytesWritable());
      }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "gunzipper");
    job.setJarByClass(GunZip.class);
    job.setMapperClass(GunzipMapper.class);
    job.setInputFormatClass(CustomGzipFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(BytesWritable.class);
    job.setOutputFormatClass(BinaryOutputFormat.class);
    job.setNumReduceTasks(0);
    CustomGzipFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

class CustomGzipFormat extends FileInputFormat<Text, Text>{
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
				return fullInputPath;
			}

			@Override
			public float getProgress() throws IOException, InterruptedException {
				return next ? 0 : 1;
			}

			private Text fullInputPath;
			private Text fileName;
			
			@Override
			public void initialize(InputSplit split, TaskAttemptContext attempt) throws IOException, InterruptedException {
				Path inputPath = ((FileSplit)split).getPath();
				fullInputPath = new Text(inputPath.toString());
				fileName = new Text(inputPath.getName().replace(".gz", ""));
			}
			private boolean next = true;
			@Override
			public boolean nextKeyValue() throws IOException,
					InterruptedException {
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
				
			}
		};
	}
}
