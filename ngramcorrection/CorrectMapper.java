package ch.epfl.bigdata.linguisticdrift.errorcorrection;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 *  Mapper function: correct spelling error, e.g., remove accent, remove common error, remove single word.
 * @param: inputValue ->
 * 						n-grams and its frequency.
 * @param: outputValue ->
 * 						frequency of the word
 * @param: outputKey ->
 * 						('fileName';'ngram')
 * @author Tao Lin
 */

public class CorrectMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	private HashMap<String, HashMap<String, String>> ruleSet = new HashMap<String, HashMap<String, String>>();
	private HashMap<String, String> filterRule_com = new HashMap<String, String>();
	private HashMap<String, String> filterRule_part = new HashMap<String, String>();
	private HashMap<String, String> filterRule_accent = new HashMap<String, String>();
	private HashMap<String, String> filterRule_spec = new HashMap<String, String>();
	
	String[] lineSplit = null;
	List<String> partialruleSet = new ArrayList<String>();
	
	/** 
	 * Setup function: setup filter rules set.
	 * 	Since we only prefer to do a simple error correction, here we only consider some simple patterns. 
	 */
	protected void setup(Context context){
		setupFilterRule(null);
		ruleSet.put("complete", filterRule_com);
		ruleSet.put("partial", filterRule_part);
		ruleSet.put("accent", filterRule_accent);
		ruleSet.put("special", filterRule_spec);
		
	}

	/**
	 * Map function: Split the input data, and do the error correction. 
	 */
	public void map(LongWritable input, Text inValue, Context context) throws IOException, InterruptedException {
		
       	// Get lineSplit 	
		String line = inValue.toString();
		lineSplit = line.split("\\s+");
		
		// Get input file information
		FileSplit splitInfo = (FileSplit) context.getInputSplit();
		String fileName = splitInfo.getPath().getName();
		
		// filter error
		String correctResult = filterError(lineSplit[0]);
						
		// Write	
		if(correctResult.toString().length() != 0){
			context.write(new Text(fileName + ";" + correctResult), new Text(lineSplit[1]));
		}
	
	}
	
	/**
	 * For each word, check if current word satisfies filter rule. If it is, do the correction.
	 * 	Besides, since there exists several filtering rule set. For each word, we assume the word only match one filter rule. 
	 * @param line: word 
	 * @return corrected word
	 */
	private String filterError(String line){		
		// Initialization
		String final_result = line;
				
		// remove ponctuation, special letter, or the accent
		if(!final_result.matches("^[a-zA-Z0-9]*")){
			final_result = final_result.replaceAll("[\\pZ\\pS]", "");		
		}
				
		// replace words
		for(String key : ruleSet.keySet()){
			if(key == "complete"){
				HashMap<String, String> currentRule = ruleSet.get(key);
				
				if(currentRule.containsKey(final_result)){
					final_result = currentRule.get(final_result);
					break;
				}
			}
			else if(key == "partial"){
				HashMap<String, String> currentRule = ruleSet.get(key);
				
				for(int i = 0; i < partialruleSet.size(); i ++){
					String rule = partialruleSet.get(i);
					if(final_result.contains(rule)){
						final_result = final_result.replace(rule, currentRule.get(rule));
						break;
					}					
				}

			}
			else if(key == "accent"){
				for(String ori : ruleSet.get(key).keySet()){
					if(final_result.contains(ori))
						final_result = final_result.replace(ori, ruleSet.get(key).get(ori));
				}
			}
			else if(key == "special"){
				HashMap<String, String> currentRule = ruleSet.get(key);
				
				for(String regex: currentRule.keySet()){
					String REGEX = regex;
					Pattern pattern = Pattern.compile(REGEX);
					Matcher matcher = pattern.matcher(final_result);
					
					if (matcher.find()){ 
						final_result = final_result.replace(matcher.group(), currentRule.get(regex) + matcher.group().charAt(matcher.group().length() - 1));
						break;
					}
				}
			}
			
		}
		
		// Assgin key and value 
		if(final_result.length() == 0 || final_result.length() == 1){
			return "";
		}
		else{
			return final_result;	
		}
	}
		
	
	/**
	 * Setup detailed filter rule.
	 */
	public void setupFilterRule(String []rules){		
		// add rules, replace complete words.
		filterRule_com.put("so", "se");
		filterRule_com.put("pir", "par");
		filterRule_com.put("pavs", "pays");
		
		// add rules, replace part of words.
		filterRule_part.put("iii", "m");
		filterRule_part.put("ii", "n");
		filterRule_part.put("bc", "be");
		filterRule_part.put("fiile", "fille");	filterRule_part.put("êlre", "être");	
		partialruleSet.add("iii"); partialruleSet.add("ii"); partialruleSet.add("bc"); partialruleSet.add("fiile"); partialruleSet.add("êlre"); 
		
		// add rules, replace accent.
		filterRule_accent.put("à", "a"); filterRule_accent.put("â", "a"); filterRule_accent.put("ä", "a"); filterRule_accent.put("á", "a");	
		filterRule_accent.put("è", "e"); filterRule_accent.put("ê", "e"); filterRule_accent.put("é", "e"); filterRule_accent.put("ë", "e");
		filterRule_accent.put("î", "i"); filterRule_accent.put("ï", "i"); filterRule_accent.put("í", "i"); filterRule_accent.put("ì", "i");
		filterRule_accent.put("ô", "o"); filterRule_accent.put("œ", "oe");
		filterRule_accent.put("ù", "u"); filterRule_accent.put("û", "u"); filterRule_accent.put("ü", "u");
		filterRule_accent.put("ÿ", "y"); filterRule_accent.put("ç", "c");
		
		// add rules, for the special replacement.
		filterRule_spec.put("q.[a-z]", "qu");
	}

}
