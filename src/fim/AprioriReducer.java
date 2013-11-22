package fim;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.StringUtils;

import automation.ProfileLogWriter;
import automation.ProfileLogWriter.TaskType;

public class AprioriReducer extends Reducer<Text, IntWritable, Text, NullWritable> {

	private static final Log LOG = LogFactory.getLog(AprioriReducer.class);

	private final Pattern space = Pattern.compile(" ");
	private int minsup;
	private int iteration;
	private List<String[]> largeItemsets;
	private List<String[]> candidateItems;
	private boolean profile;
	private Long setupTime;
	private Long countLargeItemsTime;
	private Long candidateGenTime;

	@Override
	protected void setup(final Context context) throws IOException, InterruptedException {
		final long startTime = System.nanoTime();

		iteration = context.getConfiguration().getInt("apriori.iteration", -1);
		minsup = context.getConfiguration().getInt("apriori.reducer.minsup", -1);
		profile = context.getConfiguration().getBoolean("measure.profile", false);

		if (minsup < 0) {
			throw new IOException("Reducer could not read minimum support.");
		} else if (iteration < 0) {
			throw new IOException("Reducer could not read iteration.");
		}

		largeItemsets = new ArrayList<String[]>();
		LOG.info("Starting Apriori Reducer. Iteration: " + iteration + " and minum support: " + minsup);

		setupTime = System.nanoTime() - startTime;
	}

	@Override
	protected void reduce(final Text itemset, final Iterable<IntWritable> counts, final Context context) throws IOException, InterruptedException {
		final long startTime = System.nanoTime();

		int sumCount = 0;
		for (final IntWritable count : counts) {
			sumCount += count.get();
		}
		if (sumCount > minsup) {
			largeItemsets.add(space.split(itemset.toString().trim()));
		}
		context.progress();

		countLargeItemsTime = System.nanoTime() - startTime;
	}

	@Override
	protected void cleanup(final Context context) throws IOException, InterruptedException {
		final long startTime = System.nanoTime();

		// Generate k+1 candidate itemsets
		candidateItems = new ArrayList<String[]>();
		if (iteration == 1) {
			for (int i = 0; i < largeItemsets.size(); i++) {
				for (int j = i + 1; j < largeItemsets.size(); j++) {
					if (Integer.valueOf(largeItemsets.get(i)[0]) < Integer.valueOf(largeItemsets.get(j)[0])) {
						candidateItems.add(new String[] { largeItemsets.get(i)[0], largeItemsets.get(j)[0] });
					} else {
						candidateItems.add(new String[] { largeItemsets.get(j)[0], largeItemsets.get(i)[0] });
					}
				}
			}
		} else {
			for (int i = 0; i < largeItemsets.size(); i++) {
				for (int j = i + 1; j < largeItemsets.size(); j++) {
					final String[] itemset1 = largeItemsets.get(i);
					final String[] itemset2 = largeItemsets.get(j);
					if (compareArrays(itemset1, itemset2)) {
						final String[] itemset;
						final String item;
						if (Integer.valueOf(itemset1[itemset1.length - 1]) < Integer.valueOf(itemset2[itemset2.length - 1])) {
							itemset = itemset1;
							item = itemset2[itemset2.length - 1];
						} else {
							itemset = itemset2;
							item = itemset1[itemset1.length - 1];
						}
						final String[] candidate = Arrays.copyOf(itemset, itemset.length + 1);
						candidate[itemset.length] = item;
						candidateItems.add(candidate);
					}
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

		if (profile) {
			writeProfileLogs(context);
		}
	}

	/**
	 * 
	 * @param arr1
	 *            Array 1
	 * @param arr2
	 *            Array 2
	 * @return true if the the arrays first n-1 elements are the same where n is
	 *         the array length.
	 */
	private boolean compareArrays(final String[] arr1, final String[] arr2) {
		if (arr1.length != arr2.length) {
			LOG.error("Array size does not match. This should not happen!");
			return false;
		}
		boolean result = true;
		for (int i = 0; i < arr1.length - 1; i++) {
			if (!arr1[i].equals(arr2[i])) {
				result = false;
				break;
			}
		}
		return result;
	}

	private void writeProfileLogs(final Context context) {
		final ProfileLogWriter logwriter = new ProfileLogWriter(context.getConfiguration(), TaskType.REDUCER);
		logwriter.addProperty("Count large items time", String.valueOf(countLargeItemsTime));
		logwriter.addProperty("Setup time", String.valueOf(setupTime));
		logwriter.addProperty("Candidate generation time", String.valueOf(candidateGenTime));
		logwriter.write();
	}
}
