package ch.epfl.bigdata.outofplace;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.IntWritable;

/**
 * Define Combination Key
 * @author Tao Lin
 */

public class CombinationKey implements WritableComparable<CombinationKey> {

	private Text firstKey;
    private IntWritable secondKey;
	
	public CombinationKey() {
	    super();
	    this.firstKey = new Text();
        this.secondKey = new IntWritable();
	
	}

    public Text getFirstKey() {
        return this.firstKey;
    }
    public void setFirstKey(Text firstKey) {
        this.firstKey = firstKey;
    }
    public IntWritable getSecondKey() {
        return this.secondKey;
    }
    public void setSecondKey(IntWritable secondKey) {
        this.secondKey = secondKey;
    }
	
    public void readFields(DataInput dateInput) throws IOException {
    	  this.firstKey.readFields(dateInput);
          this.secondKey.readFields(dateInput);
    }
    
    public void write(DataOutput outPut) throws IOException {
        this.firstKey.write(outPut);
        this.secondKey.write(outPut);
    }
    
    /**
     * Define comparison strategy
     * Note: This comparison is applied to the first default sort of mapreduce, i.e., sort phase of map
     * @see java.lang.Comparable#compareTo(java.lang.Object)
     */
    
	@Override
	public int compareTo(CombinationKey combinationKey) {
		return this.firstKey.compareTo(combinationKey.getFirstKey());
	}

}