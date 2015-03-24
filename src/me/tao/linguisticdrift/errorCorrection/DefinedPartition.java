package me.tao.linguisticdrift.errorCorrection;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class DefinedPartition extends Partitioner<CombinationKey, IntWritable> {

	public DefinedPartition() {
	}

	@Override
	public int getPartition(CombinationKey key, IntWritable value, int numPartitions) {
		return (key.getFirstKey().hashCode() & Integer.MAX_VALUE) % numPartitions;
	}

}