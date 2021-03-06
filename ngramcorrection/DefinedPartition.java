package ch.epfl.bigdata.linguisticdrift.errorcorrection;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Define partition
 * 
 * @author Tao Lin
 */

public class DefinedPartition extends Partitioner<CombinationKey, Text> {

	/**
	 * @param: key-> the output key of map
	 * @param: value-> the output value of map
	 * @param: numPartitions -> the number of partition, i.e., the number of reduce task.
	 */
	
	public DefinedPartition() {
	}

	@Override
	public int getPartition(CombinationKey key, Text value, int numPartitions) {
		return (key.getFirstKey().hashCode() & Integer.MAX_VALUE) % numPartitions;
	}

}