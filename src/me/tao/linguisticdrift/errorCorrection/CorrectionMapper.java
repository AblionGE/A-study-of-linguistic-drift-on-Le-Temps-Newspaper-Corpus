package me.tao.linguisticdrift.errorCorrection;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class CorrectionMapper extends Mapper<LongWritable, Text, CombinationKey, Text>{
	
	private Text valueOfOutput = new Text();
	private IntWritable valueOfKey_1 = new IntWritable();	
	private IntWritable valueOfKey_2 = new IntWritable();	
	private CombinationKey combinationKey = new CombinationKey();
	private HashMap<String, HashMap<String, String>> ruleSet = new HashMap<String, HashMap<String, String>>();
	private HashMap<String, String> filterRule_com = new HashMap<String, String>();
	private HashMap<String, String> filterRule_part = new HashMap<String, String>();
	private HashMap<String, String> filterRule_spec = new HashMap<String, String>();

	
	String[] lineSplit = null;
	List<String> partialruleSet = new ArrayList<String>();
	
	protected void setup(Context context){
		setupFilterRule(null);
		ruleSet.put("complete", filterRule_com);
		ruleSet.put("partial", filterRule_part);
		ruleSet.put("special", filterRule_spec);
		
	}
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
       	// Get lineSplit 	
		String line = value.toString();
		lineSplit = line.split("\t");
		
		// Get input file information
		FileSplit splitInfo = (FileSplit) context.getInputSplit();
		String fileName = splitInfo.getPath().getName();
		
		// filter error
		filterError();
		
		// Set key
		combinationKey.setFirstKey(valueOfKey_1);		// The first key is the count of a word. 
		combinationKey.setSecondKey(valueOfKey_2);		// The second key is the ACSII of the first character of this word. 
														// It is a secondary sort that it will sort based on the first key firstly, and then second key.
		
		// Write information
		
		if(valueOfOutput.toString().length() != 0){
			valueOfOutput.set("(" + fileName + "," + valueOfOutput.toString() + ")");
			context.write(combinationKey, valueOfOutput);
		}
					
	}
	
	private void filterError(){
		
		// Initialization
		String final_result = lineSplit[0];
		
		// remove ponctuation, special letter, or the accent
		if(!final_result.matches("^[a-zA-Z0-9]*")){
			final_result = final_result.replaceAll("[\\pP\\pZ\\pS]", "");		
		}
		
		// replace words
		for(String key : ruleSet.keySet()){
			if(key == "complete"){
				HashMap<String, String> currentRule = ruleSet.get(key);
				
				if(currentRule.containsKey(final_result)){
					final_result = currentRule.get(final_result);
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
			valueOfKey_1.set(Integer.valueOf(lineSplit[1]));		
			valueOfKey_2.set(- Integer.MAX_VALUE);
			valueOfOutput.set("");
		}
		else{
			valueOfKey_1.set(Integer.valueOf(lineSplit[1]));		
			valueOfKey_2.set(Integer.valueOf((int) final_result.charAt(0)));
			valueOfOutput.set(final_result);	
		}
	}
	
	public void setupFilterRule(String []rules){
		
		// add rules, replace complete words.
		filterRule_com.put("so", "se");
		filterRule_com.put("pir", "par");
		filterRule_com.put("pavs", "pays");
		
		// add rules, replace part of words.
		filterRule_part.put("iii", "m");
		filterRule_part.put("ii", "n");
		filterRule_part.put("bc", "be");
		filterRule_part.put("fiile", "fille");		
		filterRule_part.put("êlre", "être");
		partialruleSet.add("iii"); partialruleSet.add("ii"); partialruleSet.add("bc"); partialruleSet.add("fiile"); partialruleSet.add("êlre"); 

		// add rules, for the special replacement.
		filterRule_spec.put("q.[a-z]", "qu");
	
	}
	
}