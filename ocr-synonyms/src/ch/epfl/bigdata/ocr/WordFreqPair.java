package ch.epfl.bigdata.ocr;

/**
 * Parse and store a word:frequency pair
 * @author nicolas
 */
public class WordFreqPair{
	public final String word;
	public final int occurrences;
	
	public WordFreqPair(String word, int occurences){
		this.word = word;
		this.occurrences = occurences;
	}
	@Override
	public String toString(){
		return word+":"+occurrences;
	}
	public static WordFreqPair fromString(String s){
		String[] temp = s.split(":");
		return new  WordFreqPair(temp[0], Integer.parseInt(temp[1]));
	}
}