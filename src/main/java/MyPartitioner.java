import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartitioner extends Partitioner<Pair, Pair> {

	@Override
	public int getPartition(Pair key, Pair value, int numPartitions) {
		int c = Character.toLowerCase(key.getTerm().toString().charAt(0));
		if (c < 'a' || c > 'z')
			return numPartitions - 1;
		return (int)Math.floor((float)(numPartitions - 2) * (c-'a')/('z'-'a'));
		
		/* Alternatively
		 * 
		String naturalKey = key.getTerm().toString();
		return naturalKey.hashCode();
		 */
	}

}
