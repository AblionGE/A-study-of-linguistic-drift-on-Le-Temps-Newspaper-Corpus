package ngram;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class Ngram {
	public static class NgramMapper extends MapReduceBase implements Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text gram = new Text();
		private int ngramSize;
		private String ngramSeparator;

		
		@Override
		public void configure(JobConf JobConf) {
			ngramSize = JobConf.getInt("ngramSize", 1);
			ngramSeparator = JobConf.get("ngramSeparator", " ");
		}
		
		private String concat(Collection<String> stringCollection) {
			StringBuilder concatenator = new StringBuilder();
			for (Iterator<String> iterator = stringCollection.iterator(); iterator
					.hasNext();) {
				String string = (String) iterator.next();
				concatenator.append(string);
				if (iterator.hasNext()) {
					concatenator.append(ngramSeparator);
				}
			}
			return concatenator.toString();
		}
		
		@Override
		public void map(Object inputDefinition, Text article,
				OutputCollector<Text, IntWritable> collector, Reporter reporter)
				throws IOException {
			String stringArticle = article.toString();
 			StringTokenizer wordIterator = new StringTokenizer(stringArticle);
 			Deque<String> currentNgram = new ArrayDeque<>();
 			
 			//initialize the Deque
 			for (int i = 0; i < ngramSize - 1; i++) {
 				currentNgram.add(wordIterator.nextToken());
			}
 			
			while (wordIterator.hasMoreTokens()) {
				currentNgram.add(wordIterator.nextToken());
				currentNgram.remove();
				gram.set(concat(currentNgram));
				collector.collect(gram, one);
			}
		}
		
	}
	public static class NgramReducer implements Reducer<Text, IntWritable, Text, Integer> {


		@Override
		public void close() throws IOException {
			return;
		}

		@Override
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, Integer> collector, Reporter reporter) throws IOException {
			 int sum = 0;
			 while (values.hasNext()) {
				 IntWritable value = values.next();
				 sum += value.get();
			 }
			 collector.collect(key, sum);
		}

		@Override
		public void configure(JobConf arg0) {
			return;
		}
		
	}
	
	public static void main(String[] args) {
		
	}
}
