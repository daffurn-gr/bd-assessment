import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


/* Group Pairs by the natural key.
 * Source: http://codingjunkie.net/secondary-sort/.
 */
public class MyGroupingComparator extends WritableComparator {
	
	public MyGroupingComparator() {
		super(Pair.class);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		// TODO Auto-generated method stub
		Pair first  = (Pair) a;
		Pair second = (Pair) b;
		return first.getTerm().compareTo(second.getTerm());
	}
}
