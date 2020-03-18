import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import utils.PorterStemmer;

public class MyMapper extends Mapper<LongWritable, Text, Pair, Text> {
	
	static enum Counters { NUM_RECORDS, NUM_TOKENS }

	private Text termKeyComponent         = new Text();
	private final static IntWritable zero = new IntWritable(0);
	private final static IntWritable one  = new IntWritable(1);
	private Pair termFreqencyPair         = new Pair();
	
	private Text documentTitle            = new Text();
	private Text documentLength           = new Text();
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
		this.patternsToSkip.clear();
	}
	
	/*  
	 * Input: (k, v) = (byteOoffset, documentContents).
	 * Output: <k, v> = <(term, 1), documentTitle>;
	 *         <k, v> = <(documentTitle, 0), documentLength>). 
	 */
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		// Convert document contents to lower case and get title.
		String contents = value.toString().toLowerCase();
		String title = contents.substring(0, contents.indexOf(']'));
		this.documentTitle.set(title);
		// Remove stopwords.
		for (String pattern : this.patternsToSkip) contents = contents.replaceAll(pattern, "");	
		
		// PorterStemmer object for stemming tokens.
		PorterStemmer stemmer = new PorterStemmer();
		// Tokenize article contents.
		StringTokenizer tokenizer = new StringTokenizer(contents);
		int length = 0;
		
		while (tokenizer.hasMoreTokens()) {
			String term = stemmer.stem(tokenizer.nextToken());
			// Emit <(term,1), document>.
			this.termKeyComponent.set(term);
			this.termFreqencyPair.set(this.termKeyComponent, one);
			context.write(this.termFreqencyPair, this.documentTitle);
			length++;
			// Increment Counter NUM_TOKENS.
			context.getCounter(Counters.NUM_TOKENS).increment(1);
		}
		// Emit <(documentTitle, 0), documentLength>.
		this.documentLength.set(Integer.toString(length));
		this.termFreqencyPair.set(this.documentTitle, zero);
		context.write(this.termFreqencyPair, this.documentLength);
		// Increment Counter NUM_RECORDS.
		context.getCounter(Counters.NUM_RECORDS).increment(1);	
	}	
	
}
