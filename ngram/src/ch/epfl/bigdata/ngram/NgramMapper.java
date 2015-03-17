package ch.epfl.bigdata.ngram;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * A simple mapper. Everything is handled in the {@link FileInputFormat} and corresponding {@link RecordReader}.
 * @author gbrechbu
 *
 */
public class NgramMapper extends  Mapper<IntWritable, Text, Text, IntWritable> {
	
	private static final IntWritable ONE = new IntWritable(1);
	
	private Text gram = new Text();
	private int ngramSize = 1;
	private String ngramSeparator;
	
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
	public void map(IntWritable key, Text article, Context context) throws IOException, 
		InterruptedException {
		
		String stringArticle = article.toString();
		String[] words = stringArticle.split("\\s+");
		//StringTokenizer wordIterator = new StringTokenizer(stringArticle);
		Deque<String> currentNgram = new ArrayDeque<>();
		
		//initialize the Deque
		for (int i = 0; i < ngramSize; i++) {
			//String nextToken = wordIterator.nextToken();
			//System.out.println(nextToken);
			//currentNgram.add(nextToken);
		}
		
		for (int i = 0; i < words.length; i++) {
			String word = words[i];
			String year = "{" + String.valueOf(key.get()) + "}";
			String toReturn = year + word;
			gram.set(toReturn);
			context.write(gram, ONE);
		}
	}
}
