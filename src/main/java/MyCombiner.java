import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class MyCombiner extends Reducer<Pair, Text, Pair, Text> {
	
	private Pair _key   = new Pair();
	private Text _value = new Text();
	// For term in key, map is (Document -> Frequency).
	private Map<String, Integer> termDocFrequencies = new HashMap<String, Integer>(); 
	
	/* Combines the values for terms in a given document. 
	 * Input:  <k, v> = <(term, 1), [doc1, doc2, ...]>; (k, v) = <(document,length), null>.
	 * Output: <k, v> = <(term, frequency), doc>; (k, v) = <(document,length), null>. 
	 * Code from the wcv3 Reducer class. 
	 */
	@Override
	public void reduce(Pair key, Iterable<Text> values, Context
			context) throws IOException, InterruptedException {
		
		/* Create mapping of documents to frequency of term in that document,
		 * or mapping of a document to its length.
		 */
		Iterator<Text> iter = values.iterator();
		while (iter.hasNext()) {
			Text documentTitle = iter.next();
			// <(document,length), null>. 
			if (key.getFrequency().get() == 0) {
				context.write(key, documentTitle);
			// <(term,1), [doc1, doc2, ...]>.
			} else {	
				try {
					int currentFreq = termDocFrequencies.get(documentTitle.toString());
					int updateFreq = key.getFrequency().get();
					termDocFrequencies.put(documentTitle.toString(), currentFreq+updateFreq);
				} catch (Exception e) {
					termDocFrequencies.put(documentTitle.toString(), key.getFrequency().get());
				}
			}
		}
		
		// Emit <(term,frequency), document>.
		for (Entry<String, Integer> entry : termDocFrequencies.entrySet()) {
			this._key.set(key.getTerm().toString(), entry.getValue());
			this._value.set(entry.getKey());
			context.write(this._key, this._value);
		}
	}
}
