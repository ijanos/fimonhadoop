package fim.apriori.common;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.StringUtils;

import automation.ProfileLogWriter;
import automation.ProfileLogWriter.TaskType;
import fim.apriori.singlereduce.Apriori;

public class FirstReducer extends Reducer<Text, IntWritable, Text, NullWritable> {

	private static final Log LOG = LogFactory.getLog(FirstReducer.class);

	private int minsup;
	private int iteration;
	private List<String> largeItems;
	private List<String[]> candidateItems;
	private boolean profile;
	private long reduceStart;

	private long countLargeItemsTime;
	private long candidateGenTime;
	private long completeReduceTime;
	private long completeReduceStartTime;

	@Override
	protected void setup(final Context context) throws IOException, InterruptedException {
		completeReduceStartTime = System.nanoTime();

		iteration = context.getConfiguration().getInt("apriori.iteration", -1);
		minsup = context.getConfiguration().getInt("apriori.reducer.minsup", -1);
		profile = context.getConfiguration().getBoolean("measure.profile", false);

		if (minsup < 0) {
			throw new IOException("Reducer could not read minimum support.");
		} else if (iteration < 0) {
			throw new IOException("Reducer could not read iteration.");
		}

		if (iteration > 1) {
			LOG.error("Called in the wrong iteration");
			throw new IOException("Called in the wrong iteration");
		}

		largeItems = new ArrayList<String>();
		LOG.info("Starting Apriori reducer first iteration. Minum support: " + minsup);

		reduceStart = System.nanoTime();
	}

	@Override
	protected void reduce(final Text itemset, final Iterable<IntWritable> counts, final Context context) throws IOException, InterruptedException {

		int sumCount = 0;
		for (final IntWritable count : counts) {
			sumCount += count.get();
		}

		if (sumCount > minsup) {
			largeItems.add(itemset.toString().trim());
		}
		context.progress();

	}

	@Override
	protected void cleanup(final Context context) throws IOException, InterruptedException {
		countLargeItemsTime = System.nanoTime() - reduceStart;

		final long startTime = System.nanoTime();

		// Generate k+1 candidate itemsets
		candidateItems = new ArrayList<String[]>();

		for (int i = 0; i < largeItems.size(); i++) {
			for (int j = i + 1; j < largeItems.size(); j++) {
				if (Integer.parseInt(largeItems.get(i)) < Integer.parseInt(largeItems.get(j))) {
					candidateItems.add(new String[] { largeItems.get(i), largeItems.get(j) });
				} else {
					candidateItems.add(new String[] { largeItems.get(j), largeItems.get(i) });
				}
			}
		}

		if (candidateItems.isEmpty()) {
			context.getCounter(Apriori.FinishedCounter.FINISHED).increment(1);
		} else {
			for (final String[] candidate : candidateItems) {
				context.write(new Text(StringUtils.join(" ", candidate)), null);
			}
		}

		candidateGenTime = System.nanoTime() - startTime;
		completeReduceTime = System.nanoTime() - completeReduceStartTime;
		if (profile) {
			writeProfileLogs(context);
		}
	}

	private void writeProfileLogs(final Context context) {
		final ProfileLogWriter logwriter = new ProfileLogWriter(context.getConfiguration(), TaskType.REDUCER);
		logwriter.addProperty("large item counting time", String.valueOf(countLargeItemsTime));
		logwriter.addProperty("candidate generation time", String.valueOf(candidateGenTime));
		logwriter.addProperty("complete reduce time", String.valueOf(completeReduceTime));
		logwriter.write();
	}
}
