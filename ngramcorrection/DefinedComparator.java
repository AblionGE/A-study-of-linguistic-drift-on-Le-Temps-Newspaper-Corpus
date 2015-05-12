package ch.epfl.bigdata.linguisticdrift.errorcorrection;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Define secondary sort strategy.
 * 
 * @author Tao Lin
 */

public class DefinedComparator extends WritableComparator {

	public DefinedComparator() {
        super(CombinationKey.class,true);
    }
	
	public int compare(WritableComparable combinationKeyOne, WritableComparable CombinationKeyOther) {
		CombinationKey c1 = (CombinationKey) combinationKeyOne;
		CombinationKey c2 = (CombinationKey) CombinationKeyOther;
		
		/**
		 * Ensure that the sorting data are at the same partition. If not, sort by the first key of combination key
		 * else, the data will sort by the second key. 
		 * The position of c1 and c2 determine the sorting order. Current one is ascending order.
		 */
				
		if(!c1.getFirstKey().equals(c2.getFirstKey()))
			return c1.getFirstKey().compareTo(c2.getFirstKey());
		else 
			return c1.getSecondKey().get()-c2.getSecondKey().get();
		
	}
}
