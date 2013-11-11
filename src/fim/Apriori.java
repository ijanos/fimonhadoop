package fim;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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

	private static final Log LOG = LogFactory.getLog(Apriori.class);

	private boolean running = true;
	private int iteration = 1;
	private float minsup;
	private long numberOfBaskets;

	public static enum FinishedCounter {
		FINISHED
	}

	public int calcuateMinsup(final long numberOfItems, final float minsupPercent) {
		final float result = numberOfItems * minsupPercent;
		return Math.round(result);
	}

	public void loadSettings(final Configuration conf) {
		if (conf.getInt("apriori.debug", 0) != 0) {
			// Use the LocalJobRunner, the mapper and reducer will run in the
			// main JVM. This way we can see standard input and output messages
			LOG.info("DEBUG is enabled. Running on LocalJobRunner");
			conf.set("mapred.job.tracker", "local");
		}
		minsup = conf.getFloat("apriori.minsup", 0);
		numberOfBaskets = conf.getLong("apriori.baskets", 0);
		if (minsup == 0) {
			LOG.error("Provide minimum support percentage. Example: -D apriori.minsup=0.02");
			running = false;
		}
		if (numberOfBaskets == 0) {
			LOG.error("Provide the number of baskets. Example: -D apriori.baskets=100000");
			running = false;
		}

		LOG.info("Minimum support is " + minsup);
		LOG.info("Number of baskets is " + numberOfBaskets);
	}

	public int run(final String[] args) throws IOException {
		final Configuration conf = getConf();

		loadSettings(conf);

		conf.setInt("apriori.reducer.minsup", calcuateMinsup(numberOfBaskets, minsup));

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
				URI uri;
				try {
					uri = new URI(outputBaseDir + "/Apriori-" + (iteration - 1) + "/part-r-00000");
				} catch (final URISyntaxException e) {
					LOG.error("Could not create parseable URI.\n" + e);
					break;
				}
				DistributedCache.addCacheFile(uri, job.getConfiguration());
			}

			// Setup mapper and reducer
			job.setMapperClass(AprioriMapper.class);
			job.setReducerClass(AprioriReducer.class);

			// Specify key / value
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);

			// Limit to 1 reducer
			job.setNumReduceTasks(1);

			// Input settings
			FileInputFormat.addInputPath(job, inputPath);
			job.setInputFormatClass(TextInputFormat.class);

			// Output settings
			final Path outputDir = new Path(outputBaseDir + "/Apriori-" + iteration);
			FileOutputFormat.setOutputPath(job, outputDir);
			job.setOutputFormatClass(TextOutputFormat.class);

			LOG.info("Starting Apriori iteration " + iteration);
			LOG.info("Input path: " + inputPath);
			LOG.info("Output path: " + outputDir);

			boolean success = false;
			try {
				success = job.waitForCompletion(true);
			} catch (final ClassNotFoundException e) {
				LOG.error(e);
			} catch (final InterruptedException e) {
				LOG.error(e);
			}

			if (!success) {
				LOG.error("Hadoop iteration job failed. Aborting the Apriori cumputation");
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
