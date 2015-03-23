package me.tao.linguisticdrift.errorCorrection;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class DefinedGroupSort extends WritableComparator{
	public DefinedGroupSort() {
        super(CombinationKey.class, true);
    }
	
	public int compare(WritableComparable a, WritableComparable b) {	
		CombinationKey ck1 = (CombinationKey)a;
        CombinationKey ck2 = (CombinationKey)b;
        
        return ck1.getFirstKey().compareTo(ck2.getFirstKey());
	}

}
