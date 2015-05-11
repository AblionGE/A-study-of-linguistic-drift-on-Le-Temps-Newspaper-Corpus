package ch.epfl.bigdata.linguisticdrift.errorcorrection;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Define group strategy
 * data with the same first key will be grouped into the same group.
 * 
 * @author Tao Lin
 */

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
