import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/* Pair of (term, frequency) that implements WritableComparable.
 * Altered version of TemperateAveragingPair class found at: 
 * https://github.com/bbejeck/hadoop-algorithms/blob/master/src/bbejeck/mapred/aggregation/TemperatureAveragingPair.java
 */
public class Pair implements WritableComparable<Pair>{
	
	private Text term;
	private IntWritable frequency;

	/* Constructors */
	public Pair() {
		set(new Text(), new IntWritable(0));
	}	
	public Pair(String term, int frequency) {
		set(new Text(term), new IntWritable(frequency));
	}

	
	@Override
	public void write(DataOutput out) throws IOException {
        this.term.write(out);
        this.frequency.write(out);		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.term.readFields(in);
		this.frequency.readFields(in);
	}
	
	public static Pair read(DataInput in) throws IOException {
		Pair pair = new Pair();
        pair.readFields(in);
        return pair;
    }

	@Override
	public int compareTo(Pair o) {
        int compareVal = this.term.compareTo(o.getTerm());
        if (compareVal != 0) {
            return compareVal;
        }
        return o.getFrequency().compareTo(this.frequency);
	}

	@Override
	public String toString() {
		return "Pair<" + term + ", " + frequency + ">";
	}
	
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((frequency == null) ? 0 : frequency.hashCode());
		result = prime * result + ((term == null) ? 0 : term.hashCode());
		return result;
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj) return true;
		if (obj == null || getClass() != obj.getClass()) return false;
		Pair other = (Pair) obj;
		if (frequency == null) {
			if (other.frequency != null)
				return false;
		} else if (!frequency.equals(other.frequency))
			return false;
		if (term == null) {
			if (other.term != null)
				return false;
		} else if (!term.equals(other.term))
			return false;
		return true;
	}
	
	/* Getters and setters. */
	public Text getTerm() {return term;}
	public IntWritable getFrequency() {return frequency;}
	
	public void set(String term, int frequency){
        this.term.set(term);
        this.frequency.set(frequency);
    }

    public void set(Text term, IntWritable frequency) {
        this.term = term;
        this.frequency = frequency;
    }
    
}


