import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;


public class MyReducer extends Reducer<Pair, Pair, Text, Text> {
	
	private MultipleOutputs<Text, Text> mos;
	// Class fields for intermediate data.
	private String currentTerm      = null;
	private String[] posting        = new String[2];
	private List<String[]> postings = new ArrayList<String[]>();
	// Class field for setting the output value from the reducer.
	private Text outValue = new Text();

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		mos = new MultipleOutputs<Text, Text>(context);
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		
		// Emit final postings list.
		StringBuilder sb = new StringBuilder();
		for (String[] v : this.postings) {
			for (String s : v) {
				sb.append(s);
		 		sb.append(", ");
			}
		}
		this.outValue.set(sb.toString());
		mos.write(this.currentTerm, this.outValue, "postings");
		this.postings.clear();
		mos.close();
	}
	
	/* Input:  <k, v> = <(term, freq), [(doc1, freq), (doc2, freq), ...]> for terms; 
	 *         <k, v> = <(doc, 0), (doc, length)> for documents.
	 * Output: (k, v) = (term, postingList); (k, v) = (doc, length).
	 */	
	@Override
	public void reduce(Pair key, Iterable<Pair> values, Context
			context) throws IOException, InterruptedException {
		
		// Get term and frequency from key.
		String term = key.getTerm().toString();
		int freq = key.getFrequency().get();
		
		// If new term is countered then emit the old term with its posting list.
		if ((this.currentTerm != null) && (this.currentTerm != term)) {
			
			/** StringBuilder */
			StringBuilder sb = new StringBuilder();
			for (String[] v : this.postings) {
				for (String s : v) {
					sb.append(s);
			 		sb.append(", ");
				}
			}
			this.outValue.set(sb.toString());
			mos.write("postings", this.currentTerm, this.outValue);
			this.postings.clear();				
		}
		// Update current term.
		this.currentTerm = term;
		// Add new (document, frequency) pairs to postings.
		for (Iterator<Pair> iter = values.iterator(); iter.hasNext();) {
			Pair docFreq = iter.next();
			// Output if document <k,v>-pair, where v = docLength.
			if (freq == 0) {
				this.outValue.set(docFreq.getFrequency().toString());
				mos.write("documentSizes", this.currentTerm, this.outValue);
			} else {
				this.posting[0] = docFreq.getTerm().toString();
				this.posting[1] = Integer.toString(freq);
				this.postings.add(this.posting);	
			}
		}
	}
}