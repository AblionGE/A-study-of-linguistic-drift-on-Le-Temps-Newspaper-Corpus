package me.tao.linguisticdrift.errorCorrection;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class DefinedComparator extends WritableComparator {

	public DefinedComparator() {
        super(CombinationKey.class,true);
    }
	
	public int compare(WritableComparable combinationKeyOne, WritableComparable CombinationKeyOther) {
		CombinationKey c1 = (CombinationKey) combinationKeyOne;
		CombinationKey c2 = (CombinationKey) CombinationKeyOther;
		
		if(!c1.getFirstKey().equals(c2.getFirstKey()))
			return c1.getFirstKey().compareTo(c2.getFirstKey());
		else 
			return c1.getSecondKey().get()-c2.getSecondKey().get();
		
	}
}
