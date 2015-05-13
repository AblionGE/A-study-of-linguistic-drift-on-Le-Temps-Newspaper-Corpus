package ch.epfl.bigdata.synonyms.metrics;

import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.Text;

public class CustomText extends Text{
	
	public CustomText(String s){
		super(s);
	}
	
	@Override
	public int compareTo(BinaryComparable other) {
		byte[] temp = other.getBytes();
		return compareTo(temp, 0, temp.length);
	}
	
	@Override
	public int compareTo(byte[] other, int off, int len) {
		int freqA = Integer.parseInt(new String(getBytes()).split(":")[1]);
		int freqB = Integer.parseInt(new String(other, off, len).split(":")[1]);
		return (freqA > freqB ? 1 : (freqA < freqB ? -1 : 0));
	}
	
}
