import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;


public class MyReducer extends Reducer<Pair, Text, Text, Text> {
	
	private MultipleOutputs<Text, Text> mos;
	// Class fields for setting the output emitted from the reducer.
	private Text outKey   = new Text();	
	private Text outValue = new Text();
	// Class fields for intermediate data.
	private String currentTerm      = null;
	private String[] posting        = new String[2];
	private List<String[]> postings = new ArrayList<String[]>();

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		mos = new MultipleOutputs<Text, Text>(context);
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		mos.close();
	}
	
	/* Input:  <k, v> = <(term, frequency), [doc1, doc2, ...]>; (k, v) = <(document,length), null>.
	 * Output: (k, v) = (term, postingList); (k, v) = (document, docLength).
	 */	
	@Override
	public void reduce(Pair key, Iterable<Text> values, Context
			context) throws IOException, InterruptedException {
		
		// Get term and frequency from key.
		String term = key.getTerm().toString();
		String freq = key.getFrequency().toString();
		
		// If new term is countered then emit the old term with its posting list.
		if ((this.currentTerm != null) && (this.currentTerm != term)) {
			this.outKey.set(this.currentTerm);
			this.outValue.set(this.postings.toString());
			mos.write(this.outKey, this.outValue, "document_sizes");
			this.postings.clear();				
		}
		// Update current term.
		this.currentTerm = term;
		// Add new (document, frequency) pairs to postings.
		for (Iterator<Text> iter = values.iterator(); iter.hasNext();) {
			Text termOrDocument = iter.next();		
			if (termOrDocument.equals(null)) {
				this.outValue.set(key.getFrequency().toString());
				mos.write(this.outKey, this.outValue, "postings");
			} else {
				this.posting[0] = termOrDocument.toString();
				this.posting[1] = freq;
				this.postings.add(this.posting);	
			}
		}
	}
}