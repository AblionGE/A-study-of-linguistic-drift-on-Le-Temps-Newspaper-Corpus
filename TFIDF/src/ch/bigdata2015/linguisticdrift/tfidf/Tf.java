import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.google.gson.Gson;

public class Tf {

	public static void main(String[] args) throws Exception {
		// Delete existing output dir
		Configuration conf = new Configuration();
		Path outputPathTmp = new Path(args[1]+"-tmp");
		outputPathTmp.getFileSystem(conf).delete(outputPathTmp, true);
		Path outputPath = new Path(args[1]);
		outputPath.getFileSystem(conf).delete(outputPath, true);

		// create Hadoop job
		Job job = Job.getInstance();
		job.setJarByClass(Tf.class);
		job.setJobName("Tf");

		// set mapper/reducer classes
		job.setMapperClass(TfMapperTot.class);
		job.setReducerClass(TfReducerTot.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// define input and output folders
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]+"-tmp"));

		// launch job with verbose output and wait until it finishes
		job.waitForCompletion(true);

		// ///////////
		// Second MapReduce in order to compute the tf value
		// //////////
		Gson json = new Gson();
		// Create a HashMap that links a year with its total number of words
		HashMap<String, Double> yearFreqMap = new HashMap<String, Double>();
		try {
			Path pt = new Path("outputTf-tmp/part-r-00000");
			FileSystem fs = FileSystem.get(conf);
			BufferedReader br = new BufferedReader(new InputStreamReader(
					fs.open(pt)));
			String line;
			line = br.readLine();
			while (line != null) {
				String[] yearFreq = line.split("\t");
				yearFreqMap.put(yearFreq[0], new Double(yearFreq[1]));
				line = br.readLine();
			}
			br.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		//Transform the hashMap into JSON format 
		String mapToString = json.toJson(yearFreqMap);
		//Save the HashMap in order to have acces in the mapper
		conf.set("yearFreq", mapToString);
		FileSystem fs = FileSystem.get(conf);
		Path pathMerge = new Path("outputTf-tmp/yearTotFreq.txt");
		//FileUtil.copyMerge(fs, new Path(args[1]+"-tmp"), fs, pathMerge, true, conf, "");
		/*
		 * Start the second Map
		 */
		// create Hadoop job
		Job job2 = new Job(conf);
		job2.setJarByClass(Tf.class);
		job2.setJobName("Tf");

		// set mapper/reducer classes
		job2.setMapperClass(TfMapperVal.class);

		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		// define input and output folders
		FileInputFormat.addInputPath(job2, new Path(args[0]));
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));

		// launch job with verbose output and wait until it finishes
		job2.waitForCompletion(true);
	
	}

}
