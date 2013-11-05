package fim;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 
 * Hadoop mapreduce implementation of the Apriori algorithm.
 * 
 * 
 * @author János Illés
 * 
 */
public class Apriori extends Configured implements Tool {

	public static final String candidatesFile = "candidates.txt";

	private final boolean debug;
	private boolean running = true;
	private int iteration = 1;

	public static enum FinishedCounter {
		FINISHED
	}

	public Apriori() {
		debug = true;
	}

	public int run(final String[] args) throws Exception {
		final Configuration conf = getConf();

		if (debug) {
			// Limit the job to the machine it was started on.
			// This way we can see standard input and output messages
			conf.set("mapred.job.tracker", "local");
		}

		final Path inputPath = new Path(args[0]);
		String outputBaseDir = args[1];

		// Remove trailing slash from output directory path if present
		if (outputBaseDir.substring(outputBaseDir.length() - 1).equals("/")) {
			outputBaseDir = outputBaseDir.substring(0, outputBaseDir.length() - 1);
		}

		// Delete the output directory if exists
		final Path outputBasePath = new Path(outputBaseDir);
		final FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(outputBasePath)) {
			hdfs.delete(outputBasePath, true);
		}

		while (running) {
			conf.setInt("apriori.iteration", iteration);

			// Create a new job
			final Job job = new Job(conf, "Iterative Apriori");
			job.setJarByClass(Apriori.class);

			if (iteration > 1) {
				DistributedCache.createSymlink(conf);
				DistributedCache.addCacheFile(new URI(outputBaseDir + "/Apriori-" + iteration + "/part-00000#" + candidatesFile), conf);
			}

			// Setup mapper and reducer
			job.setMapperClass(AprioriMapper.class);
			job.setReducerClass(AprioriReducer.class);

			// Specify key / value
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			// Limit to 1 reducer
			job.setNumReduceTasks(1);

			// Input settings
			FileInputFormat.addInputPath(job, inputPath);
			job.setInputFormatClass(TextInputFormat.class);

			// Output settings
			final Path outputDir = new Path(outputBaseDir + "/Apriori-" + iteration);
			FileOutputFormat.setOutputPath(job, outputDir);
			job.setOutputFormatClass(TextOutputFormat.class);

			final boolean success = job.waitForCompletion(true);
			if (!success) {
				System.err.println("Hadoop iteration job failed. Aborting the Apriori cumputation");
				return 1;
			}

			// If the reducer incremented the FINISHED counter that means
			// it is finished and we can stop the iterations of the algorithm.
			final long finished = job.getCounters().findCounter(FinishedCounter.FINISHED).getValue();
			if (finished > 0) {
				running = false;
			}

			iteration++;
		}

		return 0;
	}

	public static void main(final String[] args) throws Exception {
		if (args.length < 2) {
			System.err.println("Not enough arguments. Add input and output directories");
			System.exit(1);
		}
		System.out.println("Starting new apriori job");
		final int res = ToolRunner.run(new Configuration(), new Apriori(), args);
		System.exit(res);
	}
}
