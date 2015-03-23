package me.tao.linguisticdrift.errorCorrection;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.IntWritable;

public class CombinationKey implements WritableComparable<CombinationKey> {

	private IntWritable firstKey;
    private IntWritable secondKey;
	
	protected CombinationKey() {
	    super();
	    this.firstKey = new IntWritable();
        this.secondKey = new IntWritable();
	
	}

    public IntWritable getFirstKey() {
        return this.firstKey;
    }
    public void setFirstKey(IntWritable firstKey) {
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
    
	@Override
	public int compareTo(CombinationKey IntKey) {
		return this.firstKey.compareTo(IntKey.getFirstKey());
	}

}