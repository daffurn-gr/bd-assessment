import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import utils.PorterStemmer;

public class MyMapper extends Mapper<LongWritable, Text, Pair, Pair> {
	
	static enum Counters { NUM_RECORDS, NUM_TOKENS }
	
	// Mapping of terms to their frequency in current document.
	private Map<String, Integer> termDocFrequencies = new HashMap<String, Integer>();

	// Variables to store mapper output for each term and the document.
	private Pair termFreqencyPair         = new Pair();	
	private Pair docFreqencyPair          = new Pair();
	// Variables to store mapper output for the document.
	
	private Set<String> patternsToSkip    = new HashSet<String>();
	

	/* Parse stopwords file and add to patternsToSkip.
	 * Code from the wcv2 Map class.
	 */
	private void parseSkipFile(Path patternsFile) {
		try {
			BufferedReader fis = new BufferedReader(new FileReader(patternsFile.toString()));
			String pattern = null;
			while ((pattern = fis.readLine()) != null)
				this.patternsToSkip.add(pattern);
			fis.close();
		} catch (IOException ioe) {
			System.err.println("Caught exception while parsing the cached file ’" +
					patternsFile + "’ : " + StringUtils.stringifyException(ioe));
		}		
	}
	
	// Setup: parse stopwords file.
	@Override
	public void setup(Context context) {
		Configuration conf = context.getConfiguration();

		if (conf.getBoolean("wordcount.skip.patterns", false)) {
			URI[] patternsFiles = new URI[0];
			try {
				patternsFiles = context.getCacheFiles();
			} catch (IOException ioe) {
				System.err.println("Caught exception while getting cached files: " +
						StringUtils.stringifyException(ioe));
			}
			for (URI patternsFile : patternsFiles)
				parseSkipFile(new Path(patternsFile.getPath()));
		}
	}

	@Override
	protected void cleanup(Context context) {
		this.termDocFrequencies.clear();
		this.patternsToSkip.clear();
	}
	
	/*  
	 * Input: (k, v) = (byteOoffset, documentContents).
	 * Output: <k, v> = <(term, freq), (doc, freq)>;
	 *         <k, v> = <(doc, 0), (doc, length>). 
	 */
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		// Convert document contents to lower case and get title.
		String contents = value.toString().toLowerCase();
		String docTitle = contents.substring(0, contents.indexOf(']'));
		int length = 0; // Track document length.
		
		// Remove stopwords.
		for (String pattern : this.patternsToSkip) contents = contents.replaceAll(pattern, "");			
		// PorterStemmer object for stemming tokens.
		PorterStemmer stemmer = new PorterStemmer();
		// Tokenize article contents.
		StringTokenizer tokenizer = new StringTokenizer(contents);
		
		// Build Map of terms with their frequencies in the current document.
		while (tokenizer.hasMoreTokens()) {			
			String term = stemmer.stem(tokenizer.nextToken()); // Tokenize.
			Integer currentTermFreq = termDocFrequencies.get(term);
			// Check if new term.
			if (currentTermFreq != null) {
				int currFreq = currentTermFreq.intValue();
				termDocFrequencies.put(term, currFreq++);
			} else {
				termDocFrequencies.put(term, 1);
			} length++;
			context.getCounter(Counters.NUM_TOKENS).increment(1);}
			
		// Output <(term, freq), (doc, freq)>.
		for (Entry<String, Integer> entry : termDocFrequencies.entrySet()) {
			this.termFreqencyPair.set(entry.getKey(), entry.getValue().intValue());
			this.docFreqencyPair.set(docTitle, entry.getValue().intValue());
			context.write(this.termFreqencyPair, this.docFreqencyPair);
		} termDocFrequencies.clear(); // Clear term->freq map for next document.
		
		// Output <(doc, 0), (doc, length>). 
		this.termFreqencyPair.set(docTitle, 0);;
		this.docFreqencyPair.set(docTitle, length);
		context.write(this.termFreqencyPair, this.docFreqencyPair);
		// Increment Counter NUM_RECORDS.
		context.getCounter(Counters.NUM_RECORDS).increment(1);	
		
	}
}
