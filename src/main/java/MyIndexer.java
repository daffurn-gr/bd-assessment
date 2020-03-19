import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class MyIndexer extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {

		// 0. Instantiate a Job object; remember to pass the Driver's configuration on to the job
		Configuration conf = getConf();
		//conf.set("mapreduce.framework.name", "local");
        //conf.set("fs.defaultFS", "file:///");
		conf.set("textinputformat.record.delimiter", "\n[[");				

		// 1. Set the jar name in the job's conf; thus the Driver will know which file to send to the cluster
		Job job = Job.getInstance(conf, "Indexer");
		job.setJarByClass(MyIndexer.class);

		// 2. Set mapper and reducer classes
		job.setMapperClass(MyMapper.class);
		job.setPartitionerClass(MyPartitioner.class);
		//job.setGroupingComparatorClass(MyGroupingComparator.class);
		job.setReducerClass(MyReducer.class);

		// 3. Set final output key and value classes
		// 4. Set input and output paths; remember, these will be HDFS paths or URLs
		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
        job.setMapOutputKeyClass(Pair.class);
        job.setMapOutputValueClass(Pair.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));		
		MultipleOutputs.addNamedOutput(job, "postings", TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "documentSizes", TextOutputFormat.class, Text.class, Text.class);

		// 5. Set other misc configuration parameters (#reducer tasks, counters, env variables, etc.)
		job.getConfiguration().setBoolean("wordcount.skip.patterns", true);
		FileSystem fs = FileSystem.get(conf);
		fs.copyFromLocalFile(new Path("file:///users/pgt/0801624s/uog-bigdata/src/main/resources/stopword-list.txt"), 
				new Path("hdfs://bigdata-10.dcs.gla.ac.uk:8020/user/0801624s/stopword-list.txt"));
		job.addCacheFile(new Path("/src/main/resources/stopword-list.txt").toUri());
		job.getConfiguration().setBoolean("wordcount.skip.patterns", true);
		//job.setNumReduceTasks(2);
		
		// 6. Finally, submit the job to the cluster and wait for it to complete; set param to false if you don't want to see progress reports
		return (job.waitForCompletion(true) ? 0 : 1);
	}
	
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new Configuration(), new MyIndexer(), args));
	}

}
