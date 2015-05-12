
import java.io.IOException;
import java.io.InputStream;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 * The {@link RecordReader} going with {@link XmlFileInputFormat}. Splits at the <full_text> tag.
 * @author gbrechbu
 *
 */
public class XmlRecordReader extends RecordReader<IntWritable, Text> {
	
	private IntWritable key;
	private Text value;
	private String filename;
	private InputStream in;
	private FileSplit split;
	private boolean done;
	private int pos;
	private NodeList nList;
	private int listSize;
	private int idArticle = 0;
	@Override
	public void initialize(InputSplit argSplit, TaskAttemptContext context)
			throws IOException, InterruptedException {
		done = false;
		pos = 0;
		
		split = (FileSplit) argSplit;
		Configuration conf = context.getConfiguration();
		final Path path = split.getPath();
		FileSystem fs = path.getFileSystem(conf);
		filename = path.getName();
		in = fs.open(path);
		
		DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder dBuilder;
		Document doc;
		try {
			dBuilder = dbFactory.newDocumentBuilder();
			doc = dBuilder.parse(in);
			
			nList = doc.getElementsByTagName("full_text");
			listSize = nList.getLength();
		} catch (ParserConfigurationException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (SAXException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public void close() throws IOException {
		in.close();
	}

	@Override
	public IntWritable getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return done ? 1.0f : 0.0f;
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {		
		key = new IntWritable();
		value = new Text();
		String tempKey = filename.replaceAll("[^0-9]", "");
		key.set(Integer.parseInt(tempKey));
		
		if (pos >= listSize) {
			done = true;
			return false;
		}
		
		Node nNode = nList.item(pos);
		String nodeText = nNode.getTextContent();
		//idArticle = idArticle++;
		value.set(new Text(nodeText));
		
		pos++;
		return true;
	}
}
