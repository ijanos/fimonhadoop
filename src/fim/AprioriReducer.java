package fim;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.StringUtils;

public class AprioriReducer extends Reducer<Text, IntWritable, Text, NullWritable> {

	private final Pattern space = Pattern.compile(" ");
	private int minsup;
	private int iteration;
	private List<String[]> largeItemsets;
	private List<String[]> candidateItems;

	@Override
	protected void setup(final Context context) throws IOException, InterruptedException {
		iteration = context.getConfiguration().getInt("apriori.iteration", -1);
		largeItemsets = new ArrayList<String[]>();
	}

	@Override
	protected void reduce(final Text itemset, final Iterable<IntWritable> counts, final Context context) throws IOException, InterruptedException {
		int sumCount = 0;
		for (final IntWritable count : counts) {
			sumCount += count.get();
		}
		if (sumCount > minsup) {
			largeItemsets.add(space.split(itemset.toString().trim()));
		}
		context.progress();
	}

	@Override
	protected void cleanup(final Context context) throws IOException, InterruptedException {
		// Generate k+1 candidate itemsets
		candidateItems = new ArrayList<String[]>();
		if (iteration == 1) {
			for (int i = 0; i < largeItemsets.size(); i++) {
				for (int j = i + 1; j < largeItemsets.size(); j++) {
					candidateItems.add(new String[] { largeItemsets.get(i)[0], largeItemsets.get(j)[0] });
				}
			}
		} else {
			for (int i = 0; i < largeItemsets.size(); i++) {
				for (int j = i + 1; j < largeItemsets.size(); j++) {
					final String[] itemset1 = largeItemsets.get(i);
					final String[] itemset2 = largeItemsets.get(j);
					if (Arrays.equals(Arrays.copyOfRange(itemset1, 0, itemset1.length - 2), Arrays.copyOfRange(largeItemsets.get(j), 0, itemset2.length - 2))) {
						final String[] candidate = Arrays.copyOf(itemset1, itemset1.length + 1);
						candidate[itemset1.length] = itemset2[itemset2.length - 1];
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
	}
}
