package fim.apriori.multireduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.StringUtils;

public class LargeitemsetReducer extends Reducer<Text, IntWritable, Text, Text> {

	private static final Log LOG = LogFactory.getLog(LargeitemsetReducer.class);
	private final Pattern space = Pattern.compile(" ");

	private int minsup;
	private int iteration;
	private List<String> largeItemsets;

	@Override
	protected void setup(final Context context) throws IOException, InterruptedException {
		iteration = context.getConfiguration().getInt("apriori.iteration", -1);
		minsup = context.getConfiguration().getInt("apriori.reducer.minsup", -1);

		if (minsup < 0) {
			throw new IOException("Reducer could not read minimum support.");
		} else if (iteration < 0) {
			throw new IOException("Reducer could not read iteration.");
		}

		largeItemsets = new ArrayList<String>();
		LOG.info("Starting LargeitemsetReducer. Iteration: " + iteration + " and minum support: " + minsup);
	}

	@Override
	protected void reduce(final Text itemset, final Iterable<IntWritable> counts, final Context context) throws IOException, InterruptedException {
		int sumCount = 0;
		for (final IntWritable count : counts) {
			sumCount += count.get();
		}
		if (sumCount > minsup) {
			largeItemsets.add(itemset.toString());
		}
		context.progress();
	}

	@Override
	protected void cleanup(final Context context) throws IOException, InterruptedException {
		for (final String largeitemset : largeItemsets) {
			final String[] items = space.split(largeitemset.trim());

			final String lastItem = items[items.length - 1];
			final String key = StringUtils.join(" ", (String[]) ArrayUtils.remove(items, items.length - 1));
			context.write(new Text(key), new Text(lastItem));

		}
	}
}
