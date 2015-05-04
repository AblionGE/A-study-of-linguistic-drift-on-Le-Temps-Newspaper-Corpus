package ch.epfl.bigdata.outofplace;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/*
 * Define secondary sort strategy.
 */

public class DefinedComparator extends WritableComparator {

	public DefinedComparator() {
        super(CombinationKey.class,true);
    }
	
	public int compare(WritableComparable combinationKeyOne, WritableComparable CombinationKeyOther) {
		CombinationKey c1 = (CombinationKey) combinationKeyOne;
		CombinationKey c2 = (CombinationKey) CombinationKeyOther;
		
		/*
		 * Ensure that the sorting data are at the same partition. If not, sort by the first key of combination key
		 * else, the data will sort by the second key. 
		 * The position of c1 and c2 determine the sorting order. Current one is descending order.
		 */
				
		if(!c1.getFirstKey().equals(c2.getFirstKey()))
			return c1.getFirstKey().compareTo(c2.getFirstKey());
		else 
			return c2.getSecondKey().get()-c1.getSecondKey().get();
		
	}
}
